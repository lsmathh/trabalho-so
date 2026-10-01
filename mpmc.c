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
                memoria_compartilhada(qtd_produtores);
                break;
            case 2:
                troca_mensagem(qtd_produtores);
                break;
            default:
                fprintf(stderr, "erro: paradigma deve ser 1 ou 2\n");
                exit(1);       
        }

        return 0;
    }


    void memoria_compartilhada(int qtd_produtores){
        //declara a área de mem compartilhada
        Shared *sh;

        //lista de pid dos produtores
        pid_t produtores[MAX_PROD]; 

        //primeiro cria a area de memoria compartilhada
        int shmid = shmget(SHM_KEY, sizeof(Shared), IPC_CREAT | IPC_EXCL | 0666);
        if(shmid == -1) {
            perror("shmget");
            exit(1);
        }

        
        sh = shmat(shmid, NULL, 0);
        if(sh == (void *) -1){
            perror("shmSat");
            exit(1);
        }

        //zera as variaveis da struct Shared, inclusive as n posicoes de produtores
        memset(sh, 0, sizeof(Shared));

        sh->n_prod = qtd_produtores;

       

        //trava o pai
        pause();

       

    }


    void troca_mensagem(int qtd_produtores){
        printf("Sou o paradigma de troca de mensagem, preciso ser implementado SOS\n");
        if(qtd_produtores > 0){
            printf("Tenho %d produtores aguarando ansiosamente\n", qtd_produtores);
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