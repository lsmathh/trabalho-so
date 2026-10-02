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

        return 0;
    }


    void memoria_compartilhada(int qtd_produtores, int paradigma){
        //declara a área de mem compartilhada
        Shared *sh;

        int shmid, semid, nsems;

        nsems = (qtd_produtores + 1) * NSEMS;

        //lista de pid dos produtores
        //pid_t produtores[MAX_PROD]; 

        //primeiro cria a area de memoria compartilhada
        shmid = shmget(KEY, sizeof(Shared), IPC_CREAT | IPC_EXCL | 0666);
        if(shmid == -1) {
            perror("shmget");
            exit(1);
        }

        //atribui a area de memoria compartilhada a sh
        sh = shmat(shmid, NULL, 0);
        if(sh == (void *) -1){
            perror("shmat");
            exit(1);
        }

        //zera as variaveis da struct Shared, inclusive as n posicoes de produtores
        memset(sh, 0, sizeof(Shared));

        //precisa registrar na mem compartilhada o paradigma para os produtores e consumidores saberem qual usar
        // tambem gravar numero de produtores que o mpmc criou
        sh->paradigma = paradigma;
        sh->n_prod = qtd_produtores;

        semid = semget(KEY, nsems, IPC_CREAT | IPC_EXCL | 0666);

        //setar 1024 para os semaforos VAZIO de cada produtor, esse semaforo indica a qtd de campos vazios em cada buffer
        union semun arg;
        arg.val = 1024;

        int ctl;
        for(int i=0; i<qtd_produtores; i++){
            ctl = semctl(semid,  VAZIO(i), SETVAL, arg);
            printf("setei o valor do vazio do produtor %d, retorno de semctl foi %d\n", i, ctl);
        }
        
    }


    void troca_mensagem(int qtd_produtores, int paradigma){
        
        printf("Sou o paradigma de troca de mensagem, preciso ser implementado SOS\n");
        if(qtd_produtores > 0){
            printf("Tenho %d %d produtores aguarando ansiosamente\n", qtd_produtores, paradigma);
        }
        
    }




     // for(int i=1; i<=qtd_produtores; i++){
        //     pid_t pid = fork();
        //     if(pid == -1){
        //         perror("erro no fork");
        //         exit(1);
        //     }else if(pid == 0){
        //         produtores_loop();
        //         exit(0);

        //     }else{
        //         produtores[i] = pid;
        //     }
        // }    


         //printf("%p\n", sh);      // imprime o endereço (um número tipo 0x7f3a2000)
        //printf("%d\n", sh->n_prod);  // vai até o endereço, acessa o campo n_prod ali dentro