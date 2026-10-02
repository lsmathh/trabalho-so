#include <stdio.h>
#include "utils.h"
#include <sys/sem.h>



int main(){
    int semid;

    semid = semget(KEY, 0, 0);
    
    //vou verificar valores de semaforos
    for(int i=0; i<4;i++){
        printf("o valor do semaforo vazio do produtor %d, eh: %d\n", i, semctl(semid, VAZIO(i), GETVAL));
    }
    

    
    

    return 0;
}