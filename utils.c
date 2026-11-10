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
static struct timespec rel_inicio;
static MsgRelatorio *rel_compartilhado = NULL;

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

static double segundos_desde(struct timespec inicio) {
    struct timespec fim;
    clock_gettime(CLOCK_MONOTONIC, &fim);
    return fim.tv_sec - inicio.tv_sec + (fim.tv_nsec - inicio.tv_nsec) / 1e9;
}

void envia_relatorio(int fila_rel, int papel, int fila, int consumidas, struct timeval inicio) {
    struct timeval fim;
    gettimeofday(&fim, NULL);
    MsgRelatorio rel = { .mtype = TIPO_RELATORIO, .pid = getpid(),
        .papel = papel, .fila = fila, .consumidas = consumidas,
        .segundos = (fim.tv_sec - inicio.tv_sec) + (fim.tv_usec - inicio.tv_usec) / 1e6 };
    int resultado;
    do {
        resultado = msgsnd(fila_rel, &rel, TAM(MsgRelatorio), 0);
    } while (resultado == -1 && errno == EINTR);
    if (resultado == -1) { perror("msgsnd relatorio"); exit(EXIT_FAILURE); }
}

static void relatorio_termino_normal(void) {
    signal(SIGTERM, SIG_IGN);  /* um unico relatorio, mesmo durante shutdown */
    MsgRelatorio rel = { .mtype = TIPO_RELATORIO, .pid = getpid(),
        .papel = rel_papel, .fila = rel_fila_cons, .consumidas = consumidas_atual,
        .segundos = segundos_desde(rel_inicio) };
    if (rel_compartilhado != NULL) {
        *rel_compartilhado = rel;  /* cada filho escreve somente no seu segmento */
    } else {
        int resultado;
        do {
            resultado = msgsnd(rel_fila, &rel, TAM(MsgRelatorio), 0);
        } while (resultado == -1 && errno == EINTR);
        if (resultado == -1) { perror("msgsnd relatorio"); _exit(EXIT_FAILURE); }
    }
}

static void trata_sigterm(int sig) {
    (void) sig;
    relatorio_termino_normal();
    _exit(EXIT_SUCCESS);
}

void prepara_termino(int fila_rel, int papel, int fila, struct timeval inicio) {
    (void) inicio; /* assinatura preservada para os chamadores existentes */
    rel_fila = fila_rel;
    rel_papel = papel;
    rel_fila_cons = fila;
    consumidas_atual = 0;
    clock_gettime(CLOCK_MONOTONIC, &rel_inicio);
    if (signal(SIGTERM, trata_sigterm) == SIG_ERR) { perror("signal"); exit(EXIT_FAILURE); }
    if (atexit(relatorio_termino_normal) != 0) { perror("atexit"); exit(EXIT_FAILURE); }
}

void prepara_termino_shm(MsgRelatorio *rel, int papel, int fila) {
    rel_compartilhado = rel;
    prepara_termino(-1, papel, fila, (struct timeval) {0});
}
