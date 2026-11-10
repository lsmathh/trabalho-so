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
#include <sys/msg.h>
#include <sys/time.h>
#include <poll.h>

#include "utils.h"

/* O pai guarda os relatorios ate shutdown, mas esvazia a fila durante a execucao. */
typedef struct {
    MsgRelatorio *itens;
    int quantidade;
} Relatorios;

static void coleta_relatorios(int fila, Relatorios *r) {
    MsgRelatorio rel;
    for (;;) {
        ssize_t n = msgrcv(fila, &rel, TAM(MsgRelatorio), TIPO_RELATORIO, IPC_NOWAIT);
        if (n == -1) {
            if (errno == EINTR) continue;
            if (errno == ENOMSG) return;
            perror("msgrcv relatorio");
            exit(EXIT_FAILURE);
        }
        MsgRelatorio *novos = realloc(r->itens, (r->quantidade + 1) * sizeof(*novos));
        if (novos == NULL) { perror("realloc relatorios"); exit(EXIT_FAILURE); }
        r->itens = novos;
        r->itens[r->quantidade++] = rel;
    }
}

static void imprime_processo(const MsgRelatorio *rel) {
    printf("PID[%d] %s: %.3f s\n", rel->pid,
           rel->papel == PRODUTOR ? "produtor" : "consumidor", rel->segundos);
}

static int imprime_coletados(const Relatorios *r, int qtd_produtores) {
    int total = 0, por_fila[MAX_PROD + 1] = {0};
    for (int i = 0; i < r->quantidade; i++) {
        const MsgRelatorio *rel = &r->itens[i];
        imprime_processo(rel);
        total += rel->consumidas;
        if (rel->fila >= 1 && rel->fila <= qtd_produtores)
            por_fila[rel->fila] += rel->consumidas;
    }
    for (int i = 1; i <= qtd_produtores; i++)
        printf("Fila %d: %d mensagens consumidas\n", i, por_fila[i]);
    return total;
}

static int le_comando(char *linha, size_t tamanho, int fila_rel, Relatorios *r) {
    struct pollfd entrada = { .fd = STDIN_FILENO, .events = POLLIN };
    for (;;) {
        coleta_relatorios(fila_rel, r);
        int pronto = poll(&entrada, 1, 50);
        if (pronto > 0) return fgets(linha, tamanho, stdin) != NULL;
        if (pronto == -1 && errno != EINTR) { perror("poll stdin"); return 0; }
    }
}

static double tempo_decorrido(struct timespec inicio) {
    struct timespec fim;
    clock_gettime(CLOCK_MONOTONIC, &fim);
    return fim.tv_sec - inicio.tv_sec + (fim.tv_nsec - inicio.tv_nsec) / 1e9;
}

// bloquear antes do fork evita SIGTERM antes do filho instalar seu handler.
static void bloqueia_termino(sigset_t *anterior) {
    sigset_t conjunto;
    sigemptyset(&conjunto);
    sigaddset(&conjunto, SIGTERM);
    if (sigprocmask(SIG_BLOCK, &conjunto, anterior) == -1) {
        perror("sigprocmask"); exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "uso: %s <produtores> <paradigma>\n", argv[0]);
        return EXIT_FAILURE;
    }

    setvbuf(stdin, NULL, _IONBF, 0); /* poll deve enxergar todos os comandos */
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
            fprintf(stderr, "erro: paradigma deve ser 1 ou 2\n");
            return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

void memoria_compartilhada(int qtd_produtores, int paradigma) {
    (void) paradigma;
    if (qtd_produtores < 1 || qtd_produtores > MAX_PROD) {
        fprintf(stderr, "erro: quantidade de produtores deve ser de 1 a %d\n", MAX_PROD);
        exit(EXIT_FAILURE);
    }

    struct timespec inicio_mpmc;
    clock_gettime(CLOCK_MONOTONIC, &inicio_mpmc);
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
    MsgRelatorio **relatorios = NULL;
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
            fprintf(stderr, "erro: use produz p n_msgs ack, consome p n_msgs ack ou shutdown\n");
            continue;
        }
        if (strcmp(acao, "produz") != 0 && strcmp(acao, "consome") != 0) {
            fprintf(stderr, "erro: comando '%s' desconhecido\n", acao);
            continue;
        }
        if (produtor < 0 || produtor > qtd_produtores
            || (produtor == 0 && strcmp(acao, "consome") == 0)
            || num_msgs < 1 || num_msgs > 1024 || (ack != 1 && ack != 2)) {
            fprintf(stderr, "erro: produtor, quantidade de mensagens ou ack invalidos\n");
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

        // um registro por filho
        MsgRelatorio **novos_rel = realloc(relatorios, (qtd_filhos + 1) * sizeof(*relatorios));
        if (novos_rel == NULL) { perror("realloc relatorios"); remove_semaforo(sem_ack); erro = 1; break; }
        relatorios = novos_rel;
        int id_rel = shmget(IPC_PRIVATE, TAM_SEG, IPC_CREAT | 0600);
        if (id_rel == -1) { perror("shmget relatorio"); remove_semaforo(sem_ack); continue; }
        MsgRelatorio *rel = shmat(id_rel, NULL, 0);
        if (shmctl(id_rel, IPC_RMID, NULL) == -1) { perror("shmctl relatorio"); exit(EXIT_FAILURE); }
        if (rel == (void *) -1) { perror("shmat relatorio"); remove_semaforo(sem_ack); continue; }
        sigset_t anterior;
        bloqueia_termino(&anterior);
        fflush(NULL);
        pid_t pid = fork();
        if (pid == -1) {
            sigprocmask(SIG_SETMASK, &anterior, NULL);
            shmdt(rel);
            perror("fork");
            remove_semaforo(sem_ack);
            continue;
        }
        if (pid == 0) {
            int papel = strcmp(acao, "produz") == 0 ? PRODUTOR : CONSUMIDOR;
            prepara_termino_shm(rel, papel, papel == CONSUMIDOR ? produtor : 0);
            sigprocmask(SIG_SETMASK, &anterior, NULL);
            if (strcmp(acao, "produz") == 0) {
                exit(produz_com_ack(sh, qtd_produtores, produtor, num_msgs, sem_ack));
            } else {
                consome_func(sh, qtd_produtores, produtor, num_msgs, ack);
                exit(EXIT_SUCCESS);
            }
        }
        sigprocmask(SIG_SETMASK, &anterior, NULL);
        relatorios[qtd_filhos] = rel;
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

    for (int i = 0; i < qtd_filhos; i++) {
        imprime_processo(relatorios[i]);
        if (shmdt(relatorios[i]) == -1) { perror("shmdt relatorio"); erro = 1; }
    }
    free(relatorios);
    int total = 0;
    for (int i = 0; i < qtd_produtores; i++) {
        printf("fila %d: %d mensagens consumidas\n", i + 1, sh[i]->consumidas);
        total += sh[i]->consumidas;
    }
    // sem filhos usando as filas, não precisa adquirir os mutexs
    for (int i = 0; i < qtd_produtores; i++) {
        if (remove_segmento(sh[i], shmids[i]) == -1) erro = 1;
        if (remove_semaforo(semids[i]) == -1) erro = 1;
    }
    free(pids);
    free(acks);
    if (erro) exit(EXIT_FAILURE);

    printf("total de mensagens consumidas: %d\n", total);
    printf("tempo de operacao do mpmc: %.3f s\n", tempo_decorrido(inicio_mpmc));
}

// implementado com padrao stripped se produtor for = 0, semaforo de ack fornecido pelo chamador
int produz_com_ack(Fila *filas[], int qtd_produtores, int produtor,
                    int n_msgs, int sem_ack) {
    if (qtd_produtores < 1 || produtor < 0 || produtor > qtd_produtores) {
        fprintf(stderr, "erro: produtor %d nao existe\n", produtor);
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
        fprintf(stderr, "erro: produtor %d nao existe\n", produtor);
        exit(EXIT_FAILURE);
    }
    Fila *fila = filas[produtor - 1];
    int semid = busca_semaforo(produtor - 1);

    for (int i = 0; i < n_msgs; i++) {
        if (ack == 2) {
            if (!tenta_consumir(semid, CHEIO)) {
                fprintf(stderr, "erro ao consumir: fila %d sem mensagens disponiveis; "
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
        consumidas_atual++;
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

int cria_fila(void) {
    int id = msgget(IPC_PRIVATE, IPC_CREAT | 0666);
    if (id == -1) perror("msgget");
    return id;
}

int remove_fila(int id){
    if (id == -1) return 0;
    if (msgctl(id, IPC_RMID, NULL) == -1 && errno != EINVAL && errno != EIDRM) {
        perror("msgctl IPC_RMID");
        return -1;
    }
    return 0;

}

int imprime_relatorios(int fila_rel, int qtd_produtores) {
    Relatorios r = {0};
    coleta_relatorios(fila_rel, &r);
    int total = imprime_coletados(&r, qtd_produtores);
    free(r.itens);
    return total;
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
    (void) paradigma;
    struct timespec inicio_mpmc;
    clock_gettime(CLOCK_MONOTONIC, &inicio_mpmc);

    if(qtd_produtores < 1 || qtd_produtores > MAX_PROD) {
        fprintf(stderr, "erro: quantidade de produtores deve ser de 1 a %d\n", MAX_PROD);
        exit(EXIT_FAILURE);

    }

    int filas[qtd_produtores];
    for (int i =0; i < qtd_produtores; i++) filas[i] = -1;
    int fila_ack = -1, fila_rel = -1;

    int erro = 0;
    for (int i = 0; i < qtd_produtores; i++) {
        filas[i] = cria_fila();
        if (filas[i] == -1) { erro = 1; break; }
    }
    if(!erro) {
        fila_ack = cria_fila();
        if (fila_ack == -1) erro = 1;
    }
    if(!erro) {
        fila_rel = cria_fila();
        if (fila_rel == -1) erro = 1;
    }
    if (erro) {
        for (int i = 0; i < qtd_produtores; i ++) remove_fila(filas[i]);
        remove_fila(fila_ack);
        remove_fila(fila_rel);
        exit(EXIT_FAILURE);
    }

    char linha[100];
    char acao[10];
    int produtor, num_msgs, ack;
    pid_t *pids = NULL;
    int qtd_filhos = 0;

    Relatorios relatorios = {0};
    while(1) {
        // recolhe os filhos que já terminaram, sem esperar pelos outros
        pid_t terminado;
        while((terminado = waitpid(-1, NULL, WNOHANG)) > 0) {
            for (int i = 0; i < qtd_filhos; i++) {
                if(pids[i] == terminado) {
                    pids[i] = -1;
                    break;
                }
            }
        }
        printf(">> ");
        fflush(stdout);
        if (!le_comando(linha, sizeof(linha), fila_rel, &relatorios)) break;
    
        if (sscanf(linha, "%9s", acao) == 1 && strcmp(acao, "shutdown") == 0) {
            break;
        }
    
        if (sscanf(linha, "%9s %d %d %d", acao, &produtor, &num_msgs, &ack) != 4) {
            fprintf(stderr, "erro: use produz p n_msgs ack, consome p n_msgs ack ou shutdown\n");
            continue;
        }
        if(strcmp(acao, "produz") != 0 && strcmp(acao, "consome") != 0) {
            fprintf(stderr, "erro: comando '%s' desconhecido\n", acao);
            continue;
        }
    
        if(produtor > qtd_produtores) {
            fprintf(stderr, "erro: fila %d nao existe\n", produtor);
            continue;
        }
    
        if (produtor < 0 || (produtor == 0 && strcmp(acao, "consome") == 0) || num_msgs < 1 || num_msgs > 1024 || (ack != 1 && ack != 2)) {
            fprintf(stderr, "erro: produtor, quantidade de mensagens ou ack invalidos\n");
            continue;
        }
    
        pid_t *novos_pids = realloc(pids, (qtd_filhos + 1) * sizeof(*pids));
        if (novos_pids == NULL) { perror("realloc pids"); erro = 1; break;}
        pids = novos_pids;
    
        sigset_t anterior;
        bloqueia_termino(&anterior);
        fflush(NULL);
        pid_t pid = fork();
        if (pid == -1) {
            sigprocmask(SIG_SETMASK, &anterior, NULL);
            perror("fork");
            continue;
        }
        if (pid == 0) {
            struct timeval inicio;
            gettimeofday(&inicio, NULL);
            int papel = (strcmp(acao, "produz") == 0) ? PRODUTOR : CONSUMIDOR;
            prepara_termino(fila_rel, papel, (papel == CONSUMIDOR) ? produtor : 0, inicio);
            sigprocmask(SIG_SETMASK, &anterior, NULL);
            if (strcmp(acao, "produz") == 0) {
                exit(produz_msg(filas, qtd_produtores, produtor, num_msgs, ack, fila_ack, fila_rel));
            }else {
                consome_msg(filas, qtd_produtores, produtor, num_msgs, ack, fila_ack, fila_rel);
                exit(EXIT_SUCCESS);
            }
        }
    
        sigprocmask(SIG_SETMASK, &anterior, NULL);
        pids[qtd_filhos] = pid;
        qtd_filhos++;
    }
    // encerra todos antes de esperar, inclusive os bloqueados nas filas
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
                coleta_relatorios(fila_rel, &relatorios);
                resultado = waitpid(pids[i], NULL, WNOHANG);
                if (resultado == 0) poll(NULL, 0, 10);
            } while (resultado == 0 || (resultado == -1 && errno == EINTR));
            if (resultado == -1) {perror("waitpid"); erro = 1; }
        }
    }

    coleta_relatorios(fila_rel, &relatorios);
    int total = imprime_coletados(&relatorios, qtd_produtores);
    free(relatorios.itens);
    
    for (int i = 0; i < qtd_produtores; i ++) {
        if(remove_fila(filas[i]) == -1) erro = 1;
    }

    if(remove_fila(fila_ack) == -1) erro = 1;
    if(remove_fila(fila_rel) == -1) erro = 1;
    free(pids);

    printf("total de mensagens consumidas: %d\n", total);
    printf("tempo de operacao do mpmc: %.3f s\n", tempo_decorrido(inicio_mpmc));

    if (erro) exit(EXIT_FAILURE);

}

int produz_msg(int filas[], int qtd_produtores, int produtor, int n_msgs,
               int ack, int fila_ack, int fila_rel) {
    (void) fila_rel;

    if (qtd_produtores < 1 || qtd_produtores > MAX_PROD ||
        produtor < 0 || produtor > qtd_produtores) {
        fprintf(stderr, "erro: fila/produtor %d nao existe\n", produtor);
        return EXIT_FAILURE;
    }

    if (n_msgs < 1 || n_msgs > 1024 || (ack != 1 && ack != 2)) {
        fprintf(stderr, "erro: quantidade de mensagens ou ack invalidos\n");
        return EXIT_FAILURE;
    }

    if (ack == 1 && fila_ack == -1) {
        fprintf(stderr, "erro: fila de confirmacoes indisponivel\n");
        return EXIT_FAILURE;
    }

    const pid_t pid_produtor = getpid();

    /*
     * produtor == 0: padrão striped
     * produtor > 0: todas as mensagens vão pra fila
     */
    for (int i = 0; i < n_msgs; i++) {
        int indice_fila = (produtor == 0)
                            ? (i % qtd_produtores)
                            : (produtor - 1);

        if (filas[indice_fila] == -1) {
            fprintf(stderr, "erro: fila %d indisponivel\n", indice_fila + 1);
            return EXIT_FAILURE;
        }

        MsgDados mensagem = {0};
        mensagem.mtype = TIPO_DADO;
        mensagem.dados.pid = pid_produtor;
        mensagem.dados.seq = i + 1;
        mensagem.dados.tempo = time(NULL);
        mensagem.pede_ack = (ack == 1);

        int resultado;
        do {
            resultado = msgsnd(filas[indice_fila], &mensagem, TAM(MsgDados), 0);
        } while (resultado == -1 && errno == EINTR);

        if (resultado == -1) {
            perror("msgsnd dados");
            return EXIT_FAILURE;
        }
    }

    /* ACK=1: todas as mensagens já foram enviadas */
    if (ack == 1) {
        int confirmadas[1025] = {0};
        int total_confirmadas = 0;

        while (total_confirmadas < n_msgs) {
            MsgAck confirmacao = {0};
            ssize_t resultado;

            do {
                resultado = msgrcv(fila_ack, &confirmacao, TAM(MsgAck),
                                   (long) pid_produtor, 0);
            } while (resultado == -1 && errno == EINTR);

            if (resultado == -1) {
                perror("msgrcv ack");
                return EXIT_FAILURE;
            }

            if (confirmacao.seq >= 1 && confirmacao.seq <= n_msgs &&
                !confirmadas[confirmacao.seq]) {
                confirmadas[confirmacao.seq] = 1;
                total_confirmadas++;
            }
        }
    }

    return EXIT_SUCCESS;
}

void consome_msg(int filas[], int qtd_produtores, int produtor, int n_msgs,
                 int ack, int fila_ack, int fila_rel) {
    (void) fila_rel;

    if (qtd_produtores < 1 || qtd_produtores > MAX_PROD ||
        produtor < 1 || produtor > qtd_produtores) {
        fprintf(stderr, "erro: fila %d nao existe\n", produtor);
        exit(EXIT_FAILURE);
    }

    if (n_msgs < 1 || n_msgs > 1024 || (ack != 1 && ack != 2)) {
        fprintf(stderr, "erro: quantidade de mensagens ou ack invalidos\n");
        exit(EXIT_FAILURE);
    }

    int indice_fila = produtor - 1;
    if (filas[indice_fila] == -1) {
        fprintf(stderr, "erro: fila %d indisponivel\n", produtor);
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < n_msgs; i++) {
        MsgDados mensagem = {0};
        ssize_t resultado;

        /*
         * ACK=1: msgrcv bloqueia até aparecer uma mensagem
         * ACK=2: IPC_NOWAIT faz a chamada retornar imediatamente com ENOMSG
         */
        int flags = (ack == 2) ? IPC_NOWAIT : 0;
        do {
            resultado = msgrcv(filas[indice_fila], &mensagem, TAM(MsgDados),
                               TIPO_DADO, flags);
        } while (resultado == -1 && errno == EINTR);

        if (resultado == -1) {
            if (ack == 2 && errno == ENOMSG) {
                fprintf(stderr,
                        "erro ao consumir: fila %d sem mensagens disponiveis; "
                        "consumidas %d de %d mensagens solicitadas\n",
                        produtor, i, n_msgs);
            } else {
                perror("msgrcv dados");
            }
            exit(EXIT_FAILURE);
        }

        consumidas_atual++;

        if (mensagem.pede_ack) {
            if (fila_ack == -1 || mensagem.dados.pid <= 0) {
                fprintf(stderr, "erro: dados invalidos para envio do ACK\n");
                exit(EXIT_FAILURE);
            }

            MsgAck confirmacao = {0};
            confirmacao.mtype = (long) mensagem.dados.pid;
            confirmacao.seq = mensagem.dados.seq;

            int envio;
            do {
                envio = msgsnd(fila_ack, &confirmacao, TAM(MsgAck), 0);
            } while (envio == -1 && errno == EINTR);

            if (envio == -1) {
                perror("msgsnd ack");
                exit(EXIT_FAILURE);
            }
        }
    }
}
