#include <stdio.h>
#include "utils.h"
#include <sys/sem.h>



int main(){
    int semid;

    semid = semget(KEY, 0, 0);
    
    //vou testar uma travada de semaforo
    
    printf("vou liberar o proc bloqueado em 0");
    v(semid, 0);

    return 0;
}