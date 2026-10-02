#ifndef UTILS_H
#define UTILS_H
#define MAX_PROD 10
#define MAX_MSGS 1024
#define KEY 0X1234
#define NSEMS 4

#define MUTEX(i) ((i) * NSEMS + 0)
#define VAZIO(i) ((i) * NSEMS + 1)
#define CHEIO(i) ((i) * NSEMS + 2)
#define ACK(i)   ((i) * NSEMS + 3)

void memoria_compartilhada(int qtd_produtores, int paradigma);
void troca_mensagem(int qtd_produtores, int paradigma);
void produtores_loop();
void p(int semid, int idx);
void v(int semid, int idx);

typedef struct{
    int buf[MAX_MSGS];
    int in;
    int out;
} Prod;


typedef struct{
    Prod prod[MAX_PROD];
    int consumidas;
    int n_prod;
    int paradigma;
} Shared;

union semun {
    int              val;
    struct semid_ds *buf;
    unsigned short  *array;
};

#endif

//quando emito produz p n_msgs ack, o produz aciona o produtor[p]