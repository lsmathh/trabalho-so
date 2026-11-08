#include "utils.h"

#include <sys/sem.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <sys/msg.h>

volatile sig_atomic_t consumidas_atual = 0;   /* sem extern: aqui é a definição */
static int rel_fila = -1, rel_papel = 0, rel_fila_cons = 0;
static struct timeval rel_inicio;

void p(int semid, int idx){
    struct sembuf op;

    op.sem_num = idx;
    op.sem_op = -1;
    op.sem_flg = 0;

    while(semop(semid, &op, 1) == -1){
        //necessario caso semop seja interrompido por sinal
        if (errno == EINTR) continue;
        perror("semop P");
        exit(1);
    }
}

void v(int semid, int idx){
    struct sembuf op;

    op.sem_num = idx;
    op.sem_op = 1;
    op.sem_flg = 0;

    while(semop(semid, &op, 1) == -1){
        perror("semop V");
        exit(1);
    }
}

int tenta_consumir(int semid, int idx) {
    struct sembuf op;

        op.sem_num = idx;
        op.sem_op = -1;
        op.sem_flg = IPC_NOWAIT;

    while (semop(semid, &op, 1) == -1) {
        if (errno == EINTR) continue;
        if (errno == EAGAIN) return 0;

        perror("semop tenta_consumir");
        exit(EXIT_FAILURE);
    }

    return 1;
}

void envia_relatorio(int fila_rel, int papel, int fila, int consumidas, struct timeval inicio) {
    struct timeval fim;
    gettimeofday(&fim, NULL);

    MsgRelatorio rel;
    rel.mtype = TIPO_RELATORIO;
    rel.pid = getpid();
    rel.papel = papel;
    rel.fila = fila;
    rel.consumidas = consumidas;
    rel.segundos = (fim.tv_sec - inicio.tv_sec)
                 + (fim.tv_usec - inicio.tv_usec) / 1000000.0;

    msgsnd(fila_rel, &rel, TAM(MsgRelatorio), IPC_NOWAIT); /* nunca bloqueia */
}

static void trata_sigterm(int sig) {          /* mpmc mandou SIGTERM */
    (void) sig;
    envia_relatorio(rel_fila, rel_papel, rel_fila_cons, consumidas_atual, rel_inicio);
    _exit(EXIT_SUCCESS);
}

static void relatorio_termino_normal(void) {  /* chamado pelo exit() */
    signal(SIGTERM, SIG_IGN);                 /* evita relatório duplicado */
    envia_relatorio(rel_fila, rel_papel, rel_fila_cons, consumidas_atual, rel_inicio);
}

void prepara_termino(int fila_rel, int papel, int fila, struct timeval inicio) {
    rel_fila = fila_rel;
    rel_papel = papel;
    rel_fila_cons = fila;
    rel_inicio = inicio;
    if (signal(SIGTERM, trata_sigterm) == SIG_ERR) { perror("signal"); exit(EXIT_FAILURE); }
    if (atexit(relatorio_termino_normal) != 0) { perror("atexit"); exit(EXIT_FAILURE); }
}