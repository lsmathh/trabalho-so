#ifndef UTILS_H
#define UTILS_H
#define MAX_PROD 10
#define MAX_MSGS 1024
#define SHM_KEY 0X1234

void memoria_compartilhada(int qtd_produtores);
void troca_mensagem(int qtd_produtores);
void produtores_loop();

typedef struct{
    int buf[MAX_MSGS];
    int in;
    int out;
    int ack;
    int n_msgs;
} Prod;


typedef struct{
    Prod prod[MAX_PROD];
    int consumidas;
    int n_prod;
} Shared;


#endif

//quando emito produz p n_msgs ack, o produz aciona o produtor[p]