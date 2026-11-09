/*
 * Testes unitarios da troca de mensagens (paradigma 2).
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

int main(void) {
    printf("Testes da troca de mensagens\n\n");

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
