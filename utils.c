#include "utils.h"
#include <sys/sem.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>


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