/*
 * Testes unitarios do mpmc: memoria compartilhada (paradigma 1) e troca de mensagens (paradigma 2).
 *
 * Cada teste segue o roteiro: prepara as filas -> executa a funcao -> confere -> limpa.
 * O main do mpmc.c e renomeado na compilacao (-Dmain=mpmc_main), entao aqui
 * chamamos as funcoes diretamente. Rode com: make teste
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/wait.h>
#include <sys/time.h>

#include "../utils.h"

#define QTD_FILAS 3

static int falhas = 0;
static int verificacoes = 0;

// confere uma condicao; se falhar, mostra a linha e continua (para a limpeza sempre rodar)
#define CONFERE(cond, descricao) do {                                          \
        verificacoes++;                                                        \
        if (!(cond)) {                                                         \
            falhas++;                                                          \
            printf("    FALHOU (linha %d): %s\n", __LINE__, descricao);        \
        }                                                                      \
    } while (0)

typedef struct {
    int dados[QTD_FILAS];
    int ack;
    int rel;
} Filas;

static Filas cria_filas(void) {
    Filas f;
    for (int i = 0; i < QTD_FILAS; i++) f.dados[i] = cria_fila();
    f.ack = cria_fila();
    f.rel = cria_fila();
    return f;
}

static void remove_filas(Filas *f) {
    for (int i = 0; i < QTD_FILAS; i++) remove_fila(f->dados[i]);
    remove_fila(f->ack);
    remove_fila(f->rel);
}

static int mensagens_na_fila(int id) {
    struct msqid_ds info;
    if (msgctl(id, IPC_STAT, &info) == -1) return -1;
    return (int) info.msg_qnum;
}

static void pausa_ms(int ms) {
    usleep(ms * 1000);
}

// espera o filho terminar por ate 'segundos'; devolve o codigo de saida,
// -1 se nao terminou a tempo (e mata o filho) ou -2 se morreu por sinal
static int espera_filho(pid_t pid, int segundos) {
    int status;
    for (int i = 0; i < segundos * 100; i++) {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            if (WIFEXITED(status)) return WEXITSTATUS(status);
            return -2;
        }
        pausa_ms(10);
    }
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
    return -1;
}

static int ainda_rodando(pid_t pid) {
    return waitpid(pid, NULL, WNOHANG) == 0;
}

static pid_t cria_filho(void) {
    fflush(NULL);   // evita que o filho repita o que estava no buffer do printf
    return fork();
}

static void inicio_teste(const char *nome) {
    printf("- %s\n", nome);
    fflush(stdout);
}

/* ------------------------------------------------------------------------ */

static void teste_cria_e_remove_fila(void) {
    inicio_teste("cria_fila e remove_fila");
    int id = cria_fila();
    CONFERE(id != -1, "cria_fila devolve um id valido");
    CONFERE(mensagens_na_fila(id) == 0, "a fila nova comeca vazia");
    CONFERE(remove_fila(id) == 0, "remove_fila apaga a fila");
    CONFERE(mensagens_na_fila(id) == -1, "a fila nao existe mais depois de removida");
    CONFERE(remove_fila(id) == 0, "remover de novo nao e erro");
    CONFERE(remove_fila(-1) == 0, "remove_fila(-1) nao faz nada");
}

static void teste_produz_normal(void) {
    inicio_teste("produz_msg normal: 5 mensagens na fila do produtor 2");
    Filas f = cria_filas();

    CONFERE(produz_msg(f.dados, QTD_FILAS, 2, 5, 2, f.ack, f.rel) == EXIT_SUCCESS,
            "produz_msg devolve sucesso");
    CONFERE(mensagens_na_fila(f.dados[0]) == 0, "fila 1 continua vazia");
    CONFERE(mensagens_na_fila(f.dados[1]) == 5, "fila 2 recebeu as 5 mensagens");
    CONFERE(mensagens_na_fila(f.dados[2]) == 0, "fila 3 continua vazia");

    time_t agora = time(NULL);
    for (int seq = 1; seq <= 5; seq++) {
        MsgDados m;
        ssize_t r = msgrcv(f.dados[1], &m, TAM(MsgDados), 0, IPC_NOWAIT);
        CONFERE(r == (ssize_t) TAM(MsgDados), "a mensagem tem o tamanho de MsgDados");
        CONFERE(m.mtype == TIPO_DADO, "mtype e TIPO_DADO");
        CONFERE(m.dados.seq == seq, "as mensagens chegam em ordem (seq 1, 2, 3...)");
        CONFERE(m.dados.pid == getpid(), "pid e o do processo produtor");
        CONFERE(m.dados.tempo <= agora && m.dados.tempo >= agora - 5, "tempo do envio e o horario atual");
        CONFERE(m.pede_ack == 0, "com ack 2 a mensagem nao pede confirmacao");
    }
    remove_filas(&f);
}

static void teste_produz_striped(void) {
    inicio_teste("produz_msg striped: 7 mensagens em 3 filas");
    Filas f = cria_filas();

    CONFERE(produz_msg(f.dados, QTD_FILAS, 0, 7, 2, f.ack, f.rel) == EXIT_SUCCESS,
            "produz_msg devolve sucesso");
    CONFERE(mensagens_na_fila(f.dados[0]) == 3, "fila 1 recebeu 3 (seq 1, 4, 7)");
    CONFERE(mensagens_na_fila(f.dados[1]) == 2, "fila 2 recebeu 2 (seq 2, 5)");
    CONFERE(mensagens_na_fila(f.dados[2]) == 2, "fila 3 recebeu 2 (seq 3, 6)");

    int esperado[] = {1, 4, 7};
    for (int i = 0; i < 3; i++) {
        MsgDados m;
        msgrcv(f.dados[0], &m, TAM(MsgDados), 0, IPC_NOWAIT);
        CONFERE(m.dados.seq == esperado[i], "fila 1 tem as seq 1, 4 e 7, nessa ordem");
    }
    remove_filas(&f);
}

static void teste_produz_fila_inexistente(void) {
    inicio_teste("produz_msg com produtor que nao existe (mensagem de erro esperada)");
    Filas f = cria_filas();

    CONFERE(produz_msg(f.dados, QTD_FILAS, QTD_FILAS + 1, 2, 2, f.ack, f.rel) == EXIT_FAILURE,
            "produtor 4 com 3 filas devolve erro");
    for (int i = 0; i < QTD_FILAS; i++)
        CONFERE(mensagens_na_fila(f.dados[i]) == 0, "nenhuma mensagem foi enviada");
    remove_filas(&f);
}

static void teste_consome_suficiente(void) {
    inicio_teste("consome_msg com mensagens suficientes (ack 2)");
    Filas f = cria_filas();
    produz_msg(f.dados, QTD_FILAS, 1, 3, 2, f.ack, f.rel);

    consumidas_atual = 0;
    consome_msg(f.dados, QTD_FILAS, 1, 3, 2, f.ack, f.rel);

    CONFERE(mensagens_na_fila(f.dados[0]) == 0, "as 3 mensagens foram retiradas");
    CONFERE(consumidas_atual == 3, "consumidas_atual contou 3");
    CONFERE(mensagens_na_fila(f.ack) == 0, "sem ack pedido, nenhuma confirmacao foi enviada");
    remove_filas(&f);
}

static void teste_consome_ack2_insuficiente(void) {
    inicio_teste("consome_msg ack 2 com mensagens de menos (mensagem de erro esperada)");
    Filas f = cria_filas();
    produz_msg(f.dados, QTD_FILAS, 1, 2, 2, f.ack, f.rel);

    pid_t pid = cria_filho();
    if (pid == 0) {
        consome_msg(f.dados, QTD_FILAS, 1, 5, 2, f.ack, f.rel);
        _exit(EXIT_SUCCESS);   // so chega aqui se NAO deu o erro esperado
    }
    int codigo = espera_filho(pid, 3);
    CONFERE(codigo == EXIT_FAILURE, "o consumidor termina com erro, sem bloquear");
    CONFERE(mensagens_na_fila(f.dados[0]) == 0, "as 2 mensagens existentes foram consumidas antes do erro");
    remove_filas(&f);
}

static void teste_consome_ack1_bloqueia(void) {
    inicio_teste("consome_msg ack 1 bloqueia com a fila vazia e acorda quando chega mensagem");
    Filas f = cria_filas();

    pid_t pid = cria_filho();
    if (pid == 0) {
        consome_msg(f.dados, QTD_FILAS, 1, 2, 1, f.ack, f.rel);
        _exit(EXIT_SUCCESS);
    }
    pausa_ms(200);
    CONFERE(ainda_rodando(pid), "com a fila vazia, o consumidor fica bloqueado");

    produz_msg(f.dados, QTD_FILAS, 1, 1, 2, f.ack, f.rel);
    pausa_ms(200);
    CONFERE(ainda_rodando(pid), "com 1 de 2 mensagens, continua bloqueado");

    produz_msg(f.dados, QTD_FILAS, 1, 1, 2, f.ack, f.rel);
    CONFERE(espera_filho(pid, 3) == EXIT_SUCCESS, "com 2 mensagens, o consumidor termina");
    CONFERE(mensagens_na_fila(f.dados[0]) == 0, "a fila ficou vazia");
    remove_filas(&f);
}

static void teste_ack1_produtor_espera_confirmacao(void) {
    inicio_teste("produz_msg ack 1 espera as confirmacoes dos consumidores");
    Filas f = cria_filas();

    pid_t produtor = cria_filho();
    if (produtor == 0) {
        _exit(produz_msg(f.dados, QTD_FILAS, 1, 2, 1, f.ack, f.rel));
    }
    pausa_ms(200);
    CONFERE(mensagens_na_fila(f.dados[0]) == 2, "o produtor enviou as 2 mensagens");
    CONFERE(ainda_rodando(produtor), "sem consumo, o produtor fica esperando a confirmacao");

    MsgDados m;
    msgrcv(f.dados[0], &m, TAM(MsgDados), TIPO_DADO, IPC_NOWAIT);
    CONFERE(m.pede_ack == 1, "com ack 1 a mensagem pede confirmacao");
    CONFERE(m.dados.pid == produtor, "o pid da mensagem e o do produtor");
    msgsnd(f.dados[0], &m, TAM(MsgDados), 0);   // devolve a mensagem para a fila

    consome_msg(f.dados, QTD_FILAS, 1, 1, 1, f.ack, f.rel);
    pausa_ms(200);
    CONFERE(ainda_rodando(produtor), "com 1 de 2 confirmacoes, o produtor continua esperando");

    consome_msg(f.dados, QTD_FILAS, 1, 1, 1, f.ack, f.rel);
    CONFERE(espera_filho(produtor, 3) == EXIT_SUCCESS, "com as 2 confirmacoes, o produtor termina");
    CONFERE(mensagens_na_fila(f.ack) == 0, "o produtor leu todas as confirmacoes");
    remove_filas(&f);
}

static void teste_ack_vai_para_o_produtor_certo(void) {
    inicio_teste("confirmacoes chegam ao produtor certo (tipo = pid)");
    Filas f = cria_filas();

    pid_t a = cria_filho();
    if (a == 0) _exit(produz_msg(f.dados, QTD_FILAS, 1, 1, 1, f.ack, f.rel));
    pid_t b = cria_filho();
    if (b == 0) _exit(produz_msg(f.dados, QTD_FILAS, 2, 1, 1, f.ack, f.rel));
    pausa_ms(200);

    consome_msg(f.dados, QTD_FILAS, 1, 1, 1, f.ack, f.rel);
    CONFERE(espera_filho(a, 3) == EXIT_SUCCESS, "o produtor A termina com a confirmacao dele");
    pausa_ms(200);
    CONFERE(ainda_rodando(b), "o produtor B continua esperando (a confirmacao de A nao era dele)");

    consome_msg(f.dados, QTD_FILAS, 2, 1, 1, f.ack, f.rel);
    CONFERE(espera_filho(b, 3) == EXIT_SUCCESS, "o produtor B termina com a confirmacao dele");
    remove_filas(&f);
}

static void teste_striped_com_ack1(void) {
    inicio_teste("produz_msg striped com ack 1 recebe confirmacoes de varias filas");
    Filas f = cria_filas();

    pid_t produtor = cria_filho();
    if (produtor == 0) _exit(produz_msg(f.dados, QTD_FILAS, 0, 3, 1, f.ack, f.rel));
    pausa_ms(200);

    for (int fila = 1; fila <= QTD_FILAS; fila++)
        consome_msg(f.dados, QTD_FILAS, fila, 1, 1, f.ack, f.rel);
    CONFERE(espera_filho(produtor, 3) == EXIT_SUCCESS,
            "as confirmacoes das 3 filas liberam o produtor");
    remove_filas(&f);
}

static void teste_imprime_relatorios(void) {
    inicio_teste("envia_relatorio e imprime_relatorios (as linhas PID e Fila abaixo sao esperadas)");
    Filas f = cria_filas();
    struct timeval inicio;
    gettimeofday(&inicio, NULL);

    envia_relatorio(f.rel, PRODUTOR, 0, 0, inicio);
    envia_relatorio(f.rel, CONSUMIDOR, 1, 3, inicio);
    envia_relatorio(f.rel, CONSUMIDOR, 2, 4, inicio);
    CONFERE(mensagens_na_fila(f.rel) == 3, "os 3 relatorios estao na fila");

    int total = imprime_relatorios(f.rel, QTD_FILAS);
    CONFERE(total == 7, "o total consumido e 3 + 4 = 7");
    CONFERE(mensagens_na_fila(f.rel) == 0, "imprime_relatorios esvaziou a fila");
    CONFERE(imprime_relatorios(f.rel, QTD_FILAS) == 0, "com a fila vazia, o total e 0 e nao bloqueia");
    remove_filas(&f);
}

static void teste_relatorio_termino_normal(void) {
    inicio_teste("prepara_termino: relatorio enviado quando o filho termina normalmente");
    Filas f = cria_filas();
    produz_msg(f.dados, QTD_FILAS, 1, 2, 2, f.ack, f.rel);

    pid_t pid = cria_filho();
    if (pid == 0) {
        struct timeval inicio;
        gettimeofday(&inicio, NULL);
        consumidas_atual = 0;
        prepara_termino(f.rel, CONSUMIDOR, 1, inicio);
        consome_msg(f.dados, QTD_FILAS, 1, 2, 1, f.ack, f.rel);
        exit(EXIT_SUCCESS);   // exit (e nao _exit) para disparar o atexit
    }
    CONFERE(espera_filho(pid, 3) == EXIT_SUCCESS, "o consumidor termina");

    MsgRelatorio rel;
    ssize_t r = msgrcv(f.rel, &rel, TAM(MsgRelatorio), TIPO_RELATORIO, IPC_NOWAIT);
    CONFERE(r != -1, "o relatorio chegou");
    CONFERE(rel.pid == pid, "o relatorio e do consumidor certo");
    CONFERE(rel.papel == CONSUMIDOR, "papel CONSUMIDOR");
    CONFERE(rel.fila == 1, "fila 1");
    CONFERE(rel.consumidas == 2, "consumiu 2");
    CONFERE(rel.segundos >= 0 && rel.segundos < 3, "tempo de execucao coerente");
    CONFERE(mensagens_na_fila(f.rel) == 0, "so um relatorio (sem duplicata)");
    remove_filas(&f);
}

static void teste_relatorio_no_sigterm(void) {
    inicio_teste("prepara_termino: relatorio enviado quando o filho bloqueado recebe SIGTERM");
    Filas f = cria_filas();
    produz_msg(f.dados, QTD_FILAS, 1, 1, 2, f.ack, f.rel);

    pid_t pid = cria_filho();
    if (pid == 0) {
        struct timeval inicio;
        gettimeofday(&inicio, NULL);
        consumidas_atual = 0;
        prepara_termino(f.rel, CONSUMIDOR, 1, inicio);
        consome_msg(f.dados, QTD_FILAS, 1, 3, 1, f.ack, f.rel);   // so ha 1: bloqueia
        exit(EXIT_SUCCESS);
    }
    pausa_ms(300);
    CONFERE(ainda_rodando(pid), "o consumidor esta bloqueado esperando mais mensagens");

    kill(pid, SIGTERM);
    int codigo = espera_filho(pid, 3);
    CONFERE(codigo == EXIT_SUCCESS, "o consumidor termina ao receber SIGTERM");

    MsgRelatorio rel;
    ssize_t r = msgrcv(f.rel, &rel, TAM(MsgRelatorio), TIPO_RELATORIO, IPC_NOWAIT);
    CONFERE(r != -1, "o relatorio chegou mesmo com o processo morto pelo SIGTERM");
    CONFERE(rel.pid == pid, "o relatorio e do consumidor certo");
    CONFERE(rel.consumidas == 1, "informa a 1 mensagem consumida antes de bloquear");
    CONFERE(rel.segundos >= 0.2, "o tempo inclui o periodo bloqueado");
    CONFERE(mensagens_na_fila(f.rel) == 0, "so um relatorio (sem duplicata)");
    remove_filas(&f);
}

/* ======================== MEMORIA COMPARTILHADA ========================= */

// as filas do paradigma 1 usam chave fixa (KEY + i): nao rode o mpmc ao mesmo tempo que os testes
typedef struct {
    Fila *sh[QTD_FILAS];
    int shmids[QTD_FILAS];
    int semids[QTD_FILAS];
} Segmentos;

static Segmentos cria_segmentos(void) {
    Segmentos s;
    for (int i = 0; i < QTD_FILAS; i++) {
        s.semids[i] = cria_semaforo_fila(i);
        s.shmids[i] = cria_segmento(i);
        s.sh[i] = (s.shmids[i] == -1) ? NULL : anexa_segmento(s.shmids[i]);
    }
    return s;
}

static void remove_segmentos(Segmentos *s) {
    for (int i = 0; i < QTD_FILAS; i++) {
        remove_segmento(s->sh[i], s->shmids[i]);
        remove_semaforo(s->semids[i]);
    }
}

static int recursos_ok(Segmentos *s) {
    for (int i = 0; i < QTD_FILAS; i++)
        if (s->semids[i] == -1 || s->shmids[i] == -1 || s->sh[i] == NULL) return 0;
    return 1;
}

static int valor_sem(int semid, int idx) {
    return semctl(semid, idx, GETVAL);
}

// quantas mensagens estao esperando na fila (o semaforo CHEIO conta isso)
static int mensagens_no_segmento(Segmentos *s, int i) {
    return valor_sem(s->semids[i], CHEIO);
}

static void teste_shm_criacao(void) {
    inicio_teste("cria_semaforo_fila, cria_segmento e anexa_segmento (mensagem de erro esperada)");
    Segmentos s = cria_segmentos();
    CONFERE(recursos_ok(&s), "os 3 segmentos e os 3 conjuntos de semaforos foram criados");
    if (!recursos_ok(&s)) {
        printf("    (rode ipcrm -a e tente de novo: sobraram recursos de outra execucao)\n");
        remove_segmentos(&s);
        return;
    }

    for (int i = 0; i < QTD_FILAS; i++) {
        CONFERE(valor_sem(s.semids[i], MUTEX) == 1, "MUTEX comeca em 1");
        CONFERE(valor_sem(s.semids[i], VAZIO) == MAX_MSGS, "VAZIO comeca em 101");
        CONFERE(valor_sem(s.semids[i], CHEIO) == 0, "CHEIO comeca em 0");
        CONFERE(s.sh[i]->in == 0 && s.sh[i]->out == 0 && s.sh[i]->consumidas == 0,
                "o segmento comeca zerado (in, out e consumidas)");
        CONFERE(busca_semaforo(i) == s.semids[i], "busca_semaforo encontra o mesmo conjunto pela chave");
    }

    int repetido = cria_semaforo_fila(0);
    CONFERE(repetido == -1, "criar de novo com a mesma chave falha (IPC_EXCL)");
    if (repetido != -1) remove_semaforo(repetido);

    remove_segmentos(&s);
    CONFERE(semget(KEY, NSEMS, 0666) == -1, "depois da remocao, os semaforos nao existem mais");
    CONFERE(shmget(KEY, TAM_SEG, 0666) == -1, "depois da remocao, o segmento nao existe mais");
}

static void teste_shm_p_v_tenta_consumir(void) {
    inicio_teste("p, v e tenta_consumir");
    int id = cria_semaforo_ack();
    CONFERE(id != -1, "cria_semaforo_ack cria um semaforo");
    CONFERE(valor_sem(id, 0) == 0, "o semaforo de ack comeca em 0");

    CONFERE(tenta_consumir(id, 0) == 0, "tenta_consumir com valor 0 devolve 0, sem bloquear");
    CONFERE(valor_sem(id, 0) == 0, "e nao altera o valor");

    v(id, 0);
    CONFERE(valor_sem(id, 0) == 1, "v soma 1");
    CONFERE(tenta_consumir(id, 0) == 1, "tenta_consumir com valor 1 devolve 1");
    CONFERE(valor_sem(id, 0) == 0, "e subtrai 1");

    v(id, 0);
    v(id, 0);
    p(id, 0);
    CONFERE(valor_sem(id, 0) == 1, "dois v e um p deixam o valor em 1");

    CONFERE(remove_semaforo(id) == 0, "remove_semaforo apaga o semaforo");
    CONFERE(remove_semaforo(-1) == 0, "remove_semaforo(-1) nao faz nada");
}

static void teste_shm_indices_validos(void) {
    inicio_teste("indices_validos (mensagens de erro esperadas)");
    Fila f;
    memset(&f, 0, sizeof(f));
    CONFERE(indices_validos(&f) == 1, "in = 0 e out = 0 sao validos");
    f.in = MAX_MSGS - 1;
    f.out = MAX_MSGS - 1;
    CONFERE(indices_validos(&f) == 1, "in = 100 e out = 100 sao validos");
    f.in = MAX_MSGS;
    CONFERE(indices_validos(&f) == 0, "in = 101 e invalido");
    f.in = 0;
    f.out = -1;
    CONFERE(indices_validos(&f) == 0, "out = -1 e invalido");
}

static void teste_shm_produz_normal(void) {
    inicio_teste("produz_com_ack normal: 5 mensagens no segmento do produtor 2");
    Segmentos s = cria_segmentos();
    if (!recursos_ok(&s)) { CONFERE(0, "recursos criados"); remove_segmentos(&s); return; }

    CONFERE(produz_com_ack(s.sh, QTD_FILAS, 2, 5, -1) == EXIT_SUCCESS, "produz_com_ack devolve sucesso");
    CONFERE(s.sh[1]->in == 5, "in do segmento 2 avancou para 5");
    CONFERE(valor_sem(s.semids[1], CHEIO) == 5, "CHEIO = 5");
    CONFERE(valor_sem(s.semids[1], VAZIO) == MAX_MSGS - 5, "VAZIO = 96");
    CONFERE(valor_sem(s.semids[1], MUTEX) == 1, "MUTEX voltou a 1");
    CONFERE(mensagens_no_segmento(&s, 0) == 0 && mensagens_no_segmento(&s, 2) == 0,
            "os segmentos 1 e 3 nao foram tocados");

    time_t agora = time(NULL);
    for (int k = 0; k < 5; k++) {
        Msg m = s.sh[1]->msg[k];
        CONFERE(m.seq == k + 1, "as mensagens estao em ordem (seq 1, 2, 3...)");
        CONFERE(m.pid == getpid(), "pid e o do produtor");
        CONFERE(m.tempo <= agora && m.tempo >= agora - 5, "tempo do envio e o horario atual");
        CONFERE(s.sh[1]->sem_ack[k] == -1, "sem ack, sem_ack fica -1");
    }
    remove_segmentos(&s);
}

static void teste_shm_produz_striped(void) {
    inicio_teste("produz_com_ack striped: 7 mensagens em 3 segmentos");
    Segmentos s = cria_segmentos();
    if (!recursos_ok(&s)) { CONFERE(0, "recursos criados"); remove_segmentos(&s); return; }

    CONFERE(produz_com_ack(s.sh, QTD_FILAS, 0, 7, -1) == EXIT_SUCCESS, "produz_com_ack devolve sucesso");
    CONFERE(mensagens_no_segmento(&s, 0) == 3, "segmento 1 recebeu 3 (seq 1, 4, 7)");
    CONFERE(mensagens_no_segmento(&s, 1) == 2, "segmento 2 recebeu 2 (seq 2, 5)");
    CONFERE(mensagens_no_segmento(&s, 2) == 2, "segmento 3 recebeu 2 (seq 3, 6)");
    CONFERE(s.sh[0]->msg[0].seq == 1 && s.sh[0]->msg[1].seq == 4 && s.sh[0]->msg[2].seq == 7,
            "segmento 1 tem as seq 1, 4 e 7, nessa ordem");
    remove_segmentos(&s);
}

static void teste_shm_produz_inexistente(void) {
    inicio_teste("produz_com_ack com produtor que nao existe (mensagem de erro esperada)");
    Segmentos s = cria_segmentos();
    if (!recursos_ok(&s)) { CONFERE(0, "recursos criados"); remove_segmentos(&s); return; }

    CONFERE(produz_com_ack(s.sh, QTD_FILAS, QTD_FILAS + 1, 2, -1) == EXIT_FAILURE,
            "produtor 4 com 3 segmentos devolve erro");
    for (int i = 0; i < QTD_FILAS; i++)
        CONFERE(mensagens_no_segmento(&s, i) == 0, "nenhuma mensagem foi escrita");
    remove_segmentos(&s);
}

static void teste_shm_vetor_circular(void) {
    inicio_teste("vetor circular: enche as 101 vagas, consome e volta ao inicio");
    Segmentos s = cria_segmentos();
    if (!recursos_ok(&s)) { CONFERE(0, "recursos criados"); remove_segmentos(&s); return; }

    produz_com_ack(s.sh, QTD_FILAS, 1, MAX_MSGS, -1);
    CONFERE(valor_sem(s.semids[0], VAZIO) == 0, "com 101 mensagens, VAZIO = 0 (fila cheia)");
    CONFERE(valor_sem(s.semids[0], CHEIO) == MAX_MSGS, "CHEIO = 101");
    CONFERE(s.sh[0]->in == 0, "in deu a volta e voltou a 0");

    consome_func(s.sh, QTD_FILAS, 1, 50, 2);
    CONFERE(s.sh[0]->out == 50, "out avancou para 50");

    produz_com_ack(s.sh, QTD_FILAS, 1, 30, -1);
    CONFERE(s.sh[0]->in == 30, "in avancou para 30, reaproveitando as vagas liberadas");
    CONFERE(s.sh[0]->msg[0].seq == 1, "a vaga 0 foi reescrita com a seq 1 do segundo produtor");
    CONFERE(valor_sem(s.semids[0], CHEIO) == MAX_MSGS - 50 + 30, "CHEIO = 81");
    CONFERE(s.sh[0]->consumidas == 50, "consumidas = 50");
    remove_segmentos(&s);
}

static void teste_shm_produtor_bloqueia_cheio(void) {
    inicio_teste("produz_com_ack bloqueia com a fila cheia e continua quando abre vaga");
    Segmentos s = cria_segmentos();
    if (!recursos_ok(&s)) { CONFERE(0, "recursos criados"); remove_segmentos(&s); return; }

    pid_t pid = cria_filho();
    if (pid == 0) _exit(produz_com_ack(s.sh, QTD_FILAS, 1, MAX_MSGS + 1, -1));
    pausa_ms(300);
    CONFERE(ainda_rodando(pid), "na mensagem 102, o produtor fica bloqueado em P(VAZIO)");
    CONFERE(valor_sem(s.semids[0], CHEIO) == MAX_MSGS, "a fila esta com as 101 vagas ocupadas");

    consome_func(s.sh, QTD_FILAS, 1, 1, 1);
    CONFERE(espera_filho(pid, 3) == EXIT_SUCCESS, "com uma vaga livre, o produtor termina");
    CONFERE(valor_sem(s.semids[0], CHEIO) == MAX_MSGS, "a vaga liberada foi ocupada pela mensagem 102");
    remove_segmentos(&s);
}

static void teste_shm_consome_suficiente(void) {
    inicio_teste("consome_func com mensagens suficientes (ack 2)");
    Segmentos s = cria_segmentos();
    if (!recursos_ok(&s)) { CONFERE(0, "recursos criados"); remove_segmentos(&s); return; }
    produz_com_ack(s.sh, QTD_FILAS, 1, 3, -1);

    consome_func(s.sh, QTD_FILAS, 1, 3, 2);
    CONFERE(s.sh[0]->out == 3, "out avancou para 3");
    CONFERE(s.sh[0]->consumidas == 3, "consumidas = 3");
    CONFERE(valor_sem(s.semids[0], CHEIO) == 0, "CHEIO voltou a 0");
    CONFERE(valor_sem(s.semids[0], VAZIO) == MAX_MSGS, "VAZIO voltou a 101");
    CONFERE(valor_sem(s.semids[0], MUTEX) == 1, "MUTEX voltou a 1");
    remove_segmentos(&s);
}

static void teste_shm_consome_ack2_insuficiente(void) {
    inicio_teste("consome_func ack 2 com mensagens de menos (mensagem de erro esperada)");
    Segmentos s = cria_segmentos();
    if (!recursos_ok(&s)) { CONFERE(0, "recursos criados"); remove_segmentos(&s); return; }
    produz_com_ack(s.sh, QTD_FILAS, 1, 2, -1);

    pid_t pid = cria_filho();
    if (pid == 0) {
        consome_func(s.sh, QTD_FILAS, 1, 5, 2);
        _exit(EXIT_SUCCESS);   // so chega aqui se NAO deu o erro esperado
    }
    CONFERE(espera_filho(pid, 3) == EXIT_FAILURE, "o consumidor termina com erro, sem bloquear");
    CONFERE(s.sh[0]->consumidas == 2, "consumiu as 2 que existiam antes do erro (visto pela memoria compartilhada)");
    CONFERE(valor_sem(s.semids[0], CHEIO) == 0, "CHEIO = 0");
    remove_segmentos(&s);
}

static void teste_shm_consome_ack1_bloqueia(void) {
    inicio_teste("consome_func ack 1 bloqueia com a fila vazia e acorda quando chega mensagem");
    Segmentos s = cria_segmentos();
    if (!recursos_ok(&s)) { CONFERE(0, "recursos criados"); remove_segmentos(&s); return; }

    pid_t pid = cria_filho();
    if (pid == 0) {
        consome_func(s.sh, QTD_FILAS, 1, 2, 1);
        _exit(EXIT_SUCCESS);
    }
    pausa_ms(200);
    CONFERE(ainda_rodando(pid), "com a fila vazia, o consumidor fica bloqueado em P(CHEIO)");

    produz_com_ack(s.sh, QTD_FILAS, 1, 1, -1);
    pausa_ms(200);
    CONFERE(ainda_rodando(pid), "com 1 de 2 mensagens, continua bloqueado");

    produz_com_ack(s.sh, QTD_FILAS, 1, 1, -1);
    CONFERE(espera_filho(pid, 3) == EXIT_SUCCESS, "com 2 mensagens, o consumidor termina");
    CONFERE(s.sh[0]->consumidas == 2, "consumidas = 2");
    remove_segmentos(&s);
}

static void teste_shm_ack1_produtor(void) {
    inicio_teste("produz_com_ack ack 1 espera as confirmacoes (semaforo de ack)");
    Segmentos s = cria_segmentos();
    if (!recursos_ok(&s)) { CONFERE(0, "recursos criados"); remove_segmentos(&s); return; }
    int sem_ack = cria_semaforo_ack();

    pid_t produtor = cria_filho();
    if (produtor == 0) _exit(produz_com_ack(s.sh, QTD_FILAS, 1, 2, sem_ack));
    pausa_ms(200);
    CONFERE(valor_sem(s.semids[0], CHEIO) == 2, "o produtor escreveu as 2 mensagens");
    CONFERE(s.sh[0]->sem_ack[0] == sem_ack && s.sh[0]->sem_ack[1] == sem_ack,
            "cada vaga guardou o id do semaforo de ack do produtor");
    CONFERE(ainda_rodando(produtor), "sem consumo, o produtor fica esperando em P(ack)");

    consome_func(s.sh, QTD_FILAS, 1, 1, 1);
    pausa_ms(200);
    CONFERE(ainda_rodando(produtor), "com 1 de 2 confirmacoes, continua esperando");

    consome_func(s.sh, QTD_FILAS, 1, 1, 1);
    CONFERE(espera_filho(produtor, 3) == EXIT_SUCCESS, "com as 2 confirmacoes, o produtor termina");
    CONFERE(valor_sem(sem_ack, 0) == 0, "o produtor consumiu as 2 confirmacoes");
    remove_semaforo(sem_ack);
    remove_segmentos(&s);
}

static void teste_shm_ack_produtor_certo(void) {
    inicio_teste("dois produtores com ack 1 na mesma fila: cada um espera so as suas mensagens");
    Segmentos s = cria_segmentos();
    if (!recursos_ok(&s)) { CONFERE(0, "recursos criados"); remove_segmentos(&s); return; }
    int ack_a = cria_semaforo_ack();
    int ack_b = cria_semaforo_ack();

    pid_t a = cria_filho();
    if (a == 0) _exit(produz_com_ack(s.sh, QTD_FILAS, 1, 1, ack_a));
    pausa_ms(200);
    pid_t b = cria_filho();
    if (b == 0) _exit(produz_com_ack(s.sh, QTD_FILAS, 1, 1, ack_b));
    pausa_ms(200);

    consome_func(s.sh, QTD_FILAS, 1, 1, 1);
    CONFERE(espera_filho(a, 3) == EXIT_SUCCESS, "a 1a mensagem (de A) libera o produtor A");
    pausa_ms(200);
    CONFERE(ainda_rodando(b), "o produtor B continua esperando");

    consome_func(s.sh, QTD_FILAS, 1, 1, 1);
    CONFERE(espera_filho(b, 3) == EXIT_SUCCESS, "a 2a mensagem (de B) libera o produtor B");
    remove_semaforo(ack_a);
    remove_semaforo(ack_b);
    remove_segmentos(&s);
}

int main(void) {
    printf("=== Memoria compartilhada (paradigma 1)\n\n");

    teste_shm_criacao();
    teste_shm_p_v_tenta_consumir();
    teste_shm_indices_validos();
    teste_shm_produz_normal();
    teste_shm_produz_striped();
    teste_shm_produz_inexistente();
    teste_shm_vetor_circular();
    teste_shm_produtor_bloqueia_cheio();
    teste_shm_consome_suficiente();
    teste_shm_consome_ack2_insuficiente();
    teste_shm_consome_ack1_bloqueia();
    teste_shm_ack1_produtor();
    teste_shm_ack_produtor_certo();

    printf("\n=== Troca de mensagens (paradigma 2)\n\n");

    teste_cria_e_remove_fila();
    teste_produz_normal();
    teste_produz_striped();
    teste_produz_fila_inexistente();
    teste_consome_suficiente();
    teste_consome_ack2_insuficiente();
    teste_consome_ack1_bloqueia();
    teste_ack1_produtor_espera_confirmacao();
    teste_ack_vai_para_o_produtor_certo();
    teste_striped_com_ack1();
    teste_imprime_relatorios();
    teste_relatorio_termino_normal();
    teste_relatorio_no_sigterm();

    printf("\n%d verificacoes, %d falhas\n", verificacoes, falhas);
    if (falhas == 0) printf("TODOS OS TESTES PASSARAM\n");
    return falhas == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
