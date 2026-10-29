#ifndef UTILS_H
#define UTILS_H

#include <sys/types.h>
#include <time.h>

#define MAX_PROD 10
#define MAX_MSGS 101 // pra caber em 2048 bytes
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

_Static_assert(sizeof(Fila) <= TAM_SEG,
               "A fila excede os 2048 bytes do segmento compartilhado");


union semun {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
};

void memoria_compartilhada(int qtd_produtores, int paradigma);
void troca_mensagem(int qtd_produtores, int paradigma);
int produz_com_ack(Fila *filas[], int qtd_produtores, int produtor, int n_msgs, int sem_ack);
void consome_func(Fila *filas[], int qtd_produtores, int produtor, int n_msgs, int ack);

void p(int semid, int idx);
void v(int semid, int idx);
int tenta_consumir(int semid, int idx);
int cria_semaforo_fila(int indice);
int cria_semaforo_ack(void);
int busca_semaforo(int indice);
int remove_semaforo(int id);
int cria_segmento(int indice);
Fila *anexa_segmento(int shmid);
int remove_segmento(Fila *fila, int shmid);
int indices_validos(const Fila *fila);

#endif
