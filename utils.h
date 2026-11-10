#ifndef UTILS_H
#define UTILS_H

#include <sys/types.h>
#include <time.h>
#include  <sys/time.h>
#include <signal.h>

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

#define TIPO_DADO 1
#define TIPO_RELATORIO 1
#define PRODUTOR 1 
#define CONSUMIDOR 2

typedef struct {
    long mtype;
    Msg dados;
    int pede_ack;
} MsgDados;

typedef struct {
    long mtype;
    int seq;
} MsgAck;

typedef struct {
    long mtype;
    pid_t pid;
    int papel;
    int fila;
    int consumidas;
    double segundos;
} MsgRelatorio;

#define TAM(tipo) (sizeof(tipo) - sizeof(long))

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

int cria_fila(void);
int remove_fila(int id);
int imprime_relatorios(int fila_rel, int qtd_produtores);
int produz_msg(int filas[], int qtd_produtores, int produtor, int n_msgs, int ack, int fila_ack, int fila_rel);
void consome_msg(int filas[], int qtd_produtores, int produtor, int n_msgs, int ack, int fila_ack, int fila_rel);
extern volatile sig_atomic_t consumidas_atual;
void prepara_termino_shm(MsgRelatorio *rel, int papel, int fila);
void prepara_termino(int fila_rel, int papel, int fila, struct timeval inicio);
void envia_relatorio(int fila_rel, int papel, int fila, int consumidas, struct timeval inicio);

#endif
