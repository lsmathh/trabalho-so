#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>

#include "utils.h"

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Uso: %s <produtores> <paradigma>\n", argv[0]);
        return EXIT_FAILURE;
    }

    int produtores = atoi(argv[1]);
    int paradigma = atoi(argv[2]);

    switch (paradigma) {
        case 1:
            memoria_compartilhada(produtores, paradigma);
            break;
        case 2:
            troca_mensagem(produtores, paradigma);
            break;
        default:
            fprintf(stderr, "Erro: paradigma deve ser 1 ou 2\n");
            return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

void memoria_compartilhada(int qtd_produtores, int paradigma) {
    (void) paradigma;
    if (qtd_produtores < 1 || qtd_produtores > MAX_PROD) {
        fprintf(stderr, "Erro: quantidade de produtores deve ser de 1 a %d\n", MAX_PROD);
        exit(EXIT_FAILURE);
    }

    Fila *sh[qtd_produtores];
    int shmids[qtd_produtores], semids[qtd_produtores];
    for (int i = 0; i < qtd_produtores; i++) {
        sh[i] = NULL;
        shmids[i] = semids[i] = -1;
    }

    int erro = 0;
    for (int i = 0; i < qtd_produtores; i++) {
        semids[i] = cria_semaforo_fila(i);
        if (semids[i] == -1) { erro = 1; break; }

        shmids[i] = cria_segmento(i);
        printf("Eu sou o i: %d e meu shmid é: %d\n", i, shmids[i]);
        if (shmids[i] == -1) { erro = 1; break; }

        sh[i] = anexa_segmento(shmids[i]);
        if (sh[i] == NULL) { erro = 1; break; }
    }
    if (erro) {
        for (int i = 0; i < qtd_produtores; i++) {
            remove_segmento(sh[i], shmids[i]);
            remove_semaforo(semids[i]);
        }
        exit(EXIT_FAILURE);
    }

    char linha[100];
    char acao[10];
    int produtor, num_msgs, ack;
    pid_t *pids = NULL;
    int *acks = NULL;
    int qtd_filhos = 0;

    while (1) {
        // recolhe os filhos que já terminaram, sem esperar pelos outros
        pid_t terminado;
        while ((terminado = waitpid(-1, NULL, WNOHANG)) > 0) {
            for (int i = 0; i < qtd_filhos; i++) {
                if (pids[i] == terminado) {
                    pids[i] = -1;
                    if (remove_semaforo(acks[i]) == 0) acks[i] = -1;
                    break;
                }
            }
        }
        printf(">> ");
        fflush(stdout);
        if (fgets(linha, sizeof(linha), stdin) == NULL) break;

        if (sscanf(linha, "%9s", acao) == 1 && strcmp(acao, "shutdown") == 0) {
            break; 

        }

        if (sscanf(linha, "%9s %d %d %d", acao, &produtor, &num_msgs, &ack) != 4) {
            fprintf(stderr, "Erro: use produz p n_msgs ack, consome p n_msgs ack ou shutdown\n");
            continue;
        }
        if (strcmp(acao, "produz") != 0 && strcmp(acao, "consome") != 0) {
            fprintf(stderr, "Erro: comando '%s' desconhecido\n", acao);
            continue;
        }
        if (produtor < 0 || produtor > qtd_produtores
            || (produtor == 0 && strcmp(acao, "consome") == 0)
            || num_msgs < 1 || num_msgs > 1024 || (ack != 1 && ack != 2)) {
            fprintf(stderr, "Erro: produtor, quantidade de mensagens ou ack invalidos\n");
            continue;
        }

        // um comando novo sempre gera um processo filho novo, então sempre realoca o tamanho pro pai depois conseguir matar
        pid_t *novos_pids = realloc(pids, (qtd_filhos + 1) * sizeof(*pids));
        if (novos_pids == NULL) { perror("realloc pids"); erro = 1; break; }
        pids = novos_pids;
        int *novos_acks = realloc(acks, (qtd_filhos + 1) * sizeof(*acks));
        if (novos_acks == NULL) { perror("realloc acks"); erro = 1; break; }
        acks = novos_acks;

        int sem_ack = -1;
        if (strcmp(acao, "produz") == 0 && ack == 1) {
            // aqui o pai tem que conhecer o ack mesmo se o produtor for interrompido
            sem_ack = cria_semaforo_ack();
            if (sem_ack == -1) continue;
        }

        fflush(NULL);
        pid_t pid = fork();
        if (pid == -1) {
            perror("fork");
            remove_semaforo(sem_ack);
            continue;
        }
        if (pid == 0) {
            if (strcmp(acao, "produz") == 0) {
                exit(produz_com_ack(sh, qtd_produtores, produtor, num_msgs, sem_ack));
            } else {
                consome_func(sh, qtd_produtores, produtor, num_msgs, ack);
                exit(EXIT_SUCCESS);
            }
        }
        pids[qtd_filhos] = pid;
        acks[qtd_filhos] = sem_ack;
        qtd_filhos++;
    }

    // encerra todos antes de esperar, inclusive os bloqueados nos semaforos
    for (int i = 0; i < qtd_filhos; i++) {
        if (pids[i] > 0 && kill(pids[i], SIGTERM) == -1 && errno != ESRCH) {
            perror("kill");
            erro = 1;
        }
    }
    for (int i = 0; i < qtd_filhos; i++) {
        if (pids[i] > 0) {
            pid_t resultado;
            do {
                resultado = waitpid(pids[i], NULL, 0);
            } while (resultado == -1 && errno == EINTR);
            if (resultado == -1) { perror("waitpid"); erro = 1; }
        }
        if (remove_semaforo(acks[i]) == -1) erro = 1;
    }

    // sem filhos usando as filas, não precisa adquirir os mutexs
    for (int i = 0; i < qtd_produtores; i++) {
        if (remove_segmento(sh[i], shmids[i]) == -1) erro = 1;
        if (remove_semaforo(semids[i]) == -1) erro = 1;
    }
    free(pids);
    free(acks);
    if (erro) exit(EXIT_FAILURE);

    printf("programa finalizado, falta colocar tempo e qtd de consumidas");
}

// implementado com padrao stripped se produtor for = 0, semaforo de ack fornecido pelo chamador
int produz_com_ack(Fila *filas[], int qtd_produtores, int produtor,
                    int n_msgs, int sem_ack) {
    if (qtd_produtores < 1 || produtor < 0 || produtor > qtd_produtores) {
        fprintf(stderr, "Erro: produtor %d nao existe\n", produtor);
        return EXIT_FAILURE;
    }

    for (int i = 0; i < n_msgs; i++) {
        int indice = produtor == 0 ? i % qtd_produtores : produtor - 1;
        Fila *fila = filas[indice];
        int semid = busca_semaforo(indice);

        p(semid, VAZIO);
        p(semid, MUTEX);
        if (!indices_validos(fila)) {
            v(semid, MUTEX);
            v(semid, VAZIO);
            return EXIT_FAILURE;
        }

        fila->msg[fila->in].pid = getpid();
        fila->msg[fila->in].seq = i + 1;
        fila->msg[fila->in].tempo = time(NULL);
        fila->sem_ack[fila->in] = sem_ack;
        fila->in = (fila->in + 1) % MAX_MSGS;

        v(semid, MUTEX);
        v(semid, CHEIO);
    }

    //se ack == 1, aguarda consumo de mensagens
    if (sem_ack != -1) {
        // printf("PID[%d] aguardando consumo das mensagens\n", getpid());
        for (int i = 0; i < n_msgs; i++) p(sem_ack, 0);
        // printf("PID[%d] recebeu confirmacao: todas as mensagens foram consumidas\n", getpid());
    }
    return EXIT_SUCCESS;
}


void consome_func(Fila *filas[], int qtd_produtores, int produtor,
                  int n_msgs, int ack) {
    if (produtor < 1 || produtor > qtd_produtores) {
        fprintf(stderr, "Erro: produtor %d nao existe\n", produtor);
        exit(EXIT_FAILURE);
    }
    Fila *fila = filas[produtor - 1];
    int semid = busca_semaforo(produtor - 1);

    for (int i = 0; i < n_msgs; i++) {
        if (ack == 2) {
            if (!tenta_consumir(semid, CHEIO)) {
                fprintf(stderr, "Erro ao consumir: fila %d sem mensagens disponiveis; "
                        "consumidas %d de %d mensagens solicitadas\n", produtor, i, n_msgs);
                exit(EXIT_FAILURE);
            }
        } else {
            p(semid, CHEIO);
        }

        p(semid, MUTEX);
        if (!indices_validos(fila)) {
            v(semid, MUTEX);
            v(semid, CHEIO);
            exit(EXIT_FAILURE);
        }

        int destino_ack = fila->sem_ack[fila->out];
        fila->out = (fila->out + 1) % MAX_MSGS;
        fila->consumidas++;
        v(semid, MUTEX);
        v(semid, VAZIO);

       // printf("PID[%d] lendo mensagem %d/%d\n", getpid(), i + 1, n_msgs);
        if (destino_ack != -1) v(destino_ack, 0);
    }
}


int cria_segmento(int indice){
    int id = shmget(KEY + indice, TAM_SEG, IPC_CREAT | IPC_EXCL | 0666);
    if (id == -1) perror("shmget inicial");
    return id;
}

Fila *anexa_segmento(int shmid){
    Fila *fila = shmat(shmid, NULL, 0);
    if (fila == (void *) -1) {
        perror("shmat");
        return NULL;
    }
    memset(fila, 0, TAM_SEG);
    return fila;
}

int remove_segmento(Fila *fila, int shmid){
    int erro = 0;
    if (fila != NULL && shmdt(fila) == -1) {
        perror("shmdt");
        erro = -1;
    }
    if (shmid != -1 && shmctl(shmid, IPC_RMID, NULL) == -1) {
        perror("shmctl IPC_RMID");
        erro = -1;
    }
    return erro;
}

// obs: utilizar so se tiver o mutex adquirido dentro de consome ou produz
//essa funcao valida se in e out tem escopo aceito (devido ao tamanho de 2048, apenas é possivel 101 mensagens em cada segmento)
int indices_validos(const Fila *fila){
    if (fila->in < 0 || fila->in >= MAX_MSGS ||
        fila->out < 0 || fila->out >= MAX_MSGS) {
        fprintf(stderr, "indices invalidos: in=%d out=%d (permitido: 0 a %d)\n",
                fila->in, fila->out, MAX_MSGS - 1);
        return 0;
    }
    return 1;
}

int cria_semaforo_fila(int indice){
    int id = semget(KEY + indice, NSEMS, IPC_CREAT | IPC_EXCL | 0666);
    if (id == -1) {
        perror("semget");
        return -1;
    }
    unsigned short valores[NSEMS] = {0};
    valores[MUTEX] = 1;
    valores[VAZIO] = MAX_MSGS;
    union semun arg = { .array = valores };
    if (semctl(id, 0, SETALL, arg) == -1) {
        perror("semctl SETALL");
        semctl(id, 0, IPC_RMID);
        return -1;
    }
    return id;
}

int remove_semaforo(int id){
    if (id == -1) return 0;
    if (semctl(id, 0, IPC_RMID) == -1 && errno != EINVAL && errno != EIDRM) {
        perror("semctl IPC_RMID");
        return -1;
    }
    return 0;
}

int cria_semaforo_ack(){
    int semid;

    //cria os semáforos
    semid = semget(IPC_PRIVATE, 1, 0666);
    if(semid == -1) {
        perror("semget ack");
        return -1;
    }

    union semun arg;
    arg.val = 0;
    if (semctl(semid, 0, SETVAL, arg) == -1) {
            perror("semctl SETVAL");
            semctl(semid, 0, IPC_RMID);
            return -1;
        }


    return semid;
}

int busca_semaforo(int produtor){
    int semid;

    //busca os semáforos
    semid = semget(KEY+produtor, NSEMS, 0666);
    if(semid == -1) {
        perror("semget busca");
        exit(1);
    }

    return semid;
}

void troca_mensagem(int qtd_produtores, int paradigma) {
    (void) qtd_produtores;
    (void) paradigma;
    printf("Paradigma de troca de mensagens ainda nao implementado\n");
}

/* TODO completar o que falta do PDF
 * - medir o tempo de cada produtor, consumidor e mpmc e guardar até o shutdown
 * - manter o tempo de quem finalizou antes de shutdown (guardar em array??)
 * - imprimir esse tempo de cada pid, até os encerrados enquanto bloqueados (como capturar do bloqueado?)
 * - imprimir fila->consumidas para cada fila antes de remover os segmentos (ou apenas printo o total acumulado de todas os segs?)
 * - implementar o paradigma 2 com msgget, msgsnd e msgrcv, incluindo a limpeza deles
 */
