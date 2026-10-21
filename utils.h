#ifndef UTILS_H
#define UTILS_H

#include <sys/types.h>
#include <time.h>

#define MAX_PROD 10
#define MAX_MSGS 1024
#define TAM_SEG 2048
#define KEY 0X1234
#define NSEMS 4

#define MUTEX 0
#define VAZIO 1
#define CHEIO 2
#define ACK 3

typedef struct{
    pid_t pid;
    int seq;
    time_t tempo;
} Msg;

typedef struct{
    Msg msg[MAX_MSGS];
    int sem_ack[MAX_MSGS];
    int in;
    int out;
    int consumidas;
    time_t duracao;
} Fila;



union semun {
    int              val;
    struct semid_ds *buf;
    unsigned short  *array;
};

Fila *busca_fila(int produtor);
void memoria_compartilhada(int qtd_produtores, int paradigma);
void produz_func(int produtor, int n_msgs, int ack);
void consome_func(int produtor, int n_msgs, int ack);
void shutdown_func();
void troca_mensagem(int qtd_produtores, int paradigma);
void p(int semid, int idx);
void v(int semid, int idx);
int busca_semaforo(int produtor);
int cria_semaforo_ack(void);


#endif

//quando emito produz p n_msgs ack, o produz aciona o produtor[p]
