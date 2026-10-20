#include <stdio.h>       // printf, fprintf, perror
#include <stdlib.h>      // atoi, exit
#include <string.h>      // strerror, memset
#include <errno.h>       // errno, EINTR, EEXIST
#include <unistd.h>      // fork, pause, getpid
#include <sys/types.h>   // pid_t, key_t
#include <sys/wait.h>    // waitpid
#include <signal.h>      // sigaction, kill, SIGTERM, sig_atomic_t
#include <sys/ipc.h>     // IPC_CREAT, IPC_EXCL, key_t
#include <sys/shm.h>     // shmget, shmat, shmdt, shmctl
#include <sys/sem.h>     // semget, semop, semctl, struct sembuf
#include <sys/msg.h>     // msgget, msgctl (só usado se paradigma==2)
#include <time.h>        // clock_gettime, struct timespec
#include <sys/time.h>

#include "utils.h"


int main(int argc, char *argv[]){

    //validacao de entrada
    if(argc != 3){
        fprintf(stderr, "uso: %s <p> <paradigma>\n", argv[0]);
        exit(1);
    }

    int paradigma = atoi(argv[2]);
    int qtd_produtores = atoi(argv[1]);


    switch(paradigma){
        case 1:
            memoria_compartilhada(qtd_produtores, paradigma);
            break;
        case 2:
            troca_mensagem(qtd_produtores, paradigma);
            break;
        default:
            fprintf(stderr, "erro: paradigma deve ser 1 ou 2\n");
            exit(1);       
    }

    //colocar aqui a logica para ficar recebendos comandos >> produz/consome/shutdown
    // while(true){

    // }

    return 0;
}


void memoria_compartilhada(int qtd_produtores, int paradigma){


    //declarações
    Fila *sh[qtd_produtores];
    int shmid, semid;

    //cria segmento e semaforos para cada produtor
    for(int i=0; i<qtd_produtores; i++){

        //cria os semáforos
        semid = semget(KEY+i, NSEMS, IPC_CREAT | IPC_EXCL | 0666);
        if(semid == -1) {
            perror("semget");
            exit(1);
        }

        //cria os segmentos
        shmid = shmget(KEY+i, sizeof(Fila), IPC_CREAT | IPC_EXCL | 0666);
        printf("Eu sou o i: %d e meu shmid é: %d\n", i, shmid);
        if(shmid == -1) {
            perror("shmget");
            exit(1);
        }
        
        //faz o attach pra cada fila[i]
        sh[i] = shmat(shmid, NULL, 0);
        if(sh == (void *) -1){
            perror("shmat");
            exit(1);
        }

        //zera as variaveis da fila
        memset(sh[i], 0, sizeof(Fila));

        //setar 1024 para os semaforos VAZIO de cada produtor, esse semaforo indica a qtd de campos vazios em cada buffer
        union semun arg;
        arg.val = 1024;
        semctl(semid,  VAZIO(i), SETVAL, arg);        
    }
    
    char linha[100];
    char acao[10];
    int produtor, num_msgs, ack, pid;
    int loop = 1;

    //TODO se produtor == 0, falta implementar logica para distribuir mensagens
    while(loop){
        
        printf(">> ");
        fflush(stdout);


        if (fgets(linha, sizeof(linha), stdin) == NULL) continue;

        if (sscanf(linha, "%9s", acao) == 1 && strcmp(acao, "shutdown") == 0){
            //printf("Encerrando o programa de forma limpa...\n");

            for(int i=0; i<sh[produtor-1]->in; i++){
                printf("pid da msg[%d]: %d\n", i, sh[produtor-1]->msg[i].pid);
                printf("seq da msg[%d]: %d\n", i, sh[produtor-1]->msg[i].seq);
                printf("tempo da msg[%d]: %ld\n", i, sh[produtor-1]->msg[i].tempo);
            }
           

            printf("in: %d\n", sh[produtor-1]->in);
            printf("out: %d\n", sh[produtor-1]->out);



            
            continue;
            //shutdown_func();
        }

        int lidos = sscanf(linha, "%9s %d %d %d", acao, &produtor, &num_msgs, &ack);
        if (lidos == 4){
            if (strcmp(acao, "produz") == 0) {
                if ((pid = fork()) == 0){

                    struct timeval inicio, fim;
                    gettimeofday(&inicio, NULL);
 
                    produz_func(produtor, num_msgs, ack);


                    gettimeofday(&fim, NULL);
                    // double tempo = (double)(fim.tv_sec - inicio.tv_sec) + 
                    //             (double)(fim.tv_usec - inicio.tv_usec) / 1000000.0;

                    exit(0);
                }
            } 
            else if (strcmp(acao, "consome") == 0) {
                if ((pid = fork()) == 0){

                    struct timeval inicio, fim;
                    gettimeofday(&inicio, NULL);


                    consome_func(produtor, num_msgs, ack);


                    gettimeofday(&fim, NULL);
                    // double tempo = (double)(fim.tv_sec - inicio.tv_sec) + 
                    //             (double)(fim.tv_usec - inicio.tv_usec) / 1000000.0;

                    exit(0);
                }
            }
        }else{
            printf("Erro: Use 'produz p n_msgs ack', 'consome p n_msgs ack' ou 'shutdown'\n");
        }
    }
}
    


void troca_mensagem(int qtd_produtores, int paradigma){
    // TODO: implementar o paradigma de troca de mensagens (paradigma 2)
    //   - criar a fila de mensagens (msgget)
    //   - produtores enviam com msgsnd, consumidor recebe com msgrcv
    //   - respeitar qtd_produtores e o limite MAX_PROD
    printf("Sou o paradigma de troca de mensagem, preciso ser implementado SOS\n");
    if(qtd_produtores > 0){
        printf("Tenho %d %d produtores aguardando ansiosamente\n", qtd_produtores, paradigma);
    }
    
}


void produz_func(int produtor, int n_msgs, int ack){

    //TODO implementar padrão stripped
    Fila *sh = busca_fila(produtor);
    int sequencial = 0;


    for(int i=0; i < n_msgs; i++){

        //TODO semaforos
        //logica de semaforos aqui


        sh->msg[sh->in].pid = getpid();
        sh->msg[sh->in].seq = ++sequencial;
        sh->msg[sh->in].tempo = time(NULL);
        sh->in = (sh->in + 1) % MAX_MSGS;

        //logica de semaforos aqui
    }

    shmdt(sh);

    exit(0);
}

void consome_func(int produtor, int n_msgs, int ack){
    Fila *sh = busca_fila(produtor);
    int sequencial = 0;


    for(int i=0; i < n_msgs; i++){
        //TODO semaforos
        //logica de semaforos aqui

        sh->out = (sh->out + 1) % MAX_MSGS;

        //logica de semaforos aqui
    }

    shmdt(sh);

    exit(0);
}

void shutdown_func(){
    printf("encerrando programa\n");
    //como pegar o tempo de cada processo?
    exit(0);

}

Fila *busca_fila(int produtor){
    Fila *sh;
    int prod = produtor-1;
    int shmid;


    shmid = shmget(KEY+prod, sizeof(Fila), 0666);
    if(shmid == -1) {
        perror("shmget");
        exit(1);
    }
        

    //atribui a area de memoria compartilhada a sh
    sh = (Fila *) shmat(shmid, NULL, 0);
    if(sh == (void *) -1){
        perror("shmat");
        exit(1);
    }

    return sh;
}
