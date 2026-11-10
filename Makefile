CC = gcc
CFLAGS = -Wall -Wextra -g
FONTES = mpmc.c utils.c
CABECALHOS = utils.h

.PHONY: all clean teste teste-integracao

all: mpmc

mpmc: $(FONTES) $(CABECALHOS)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $(FONTES) $(LDLIBS) -o $@

# testes unitarios (memoria compartilhada e troca de mensagens): o main do mpmc.c e renomeado
# para que o programa de teste tenha o seu proprio main
teste: tests/test_mpmc.c $(FONTES) $(CABECALHOS)
	$(CC) $(CPPFLAGS) $(CFLAGS) -Dmain=mpmc_main -c mpmc.c -o tests/mpmc_teste.o
	$(CC) $(CPPFLAGS) $(CFLAGS) tests/test_mpmc.c tests/mpmc_teste.o utils.c $(LDLIBS) -o tests/test_mpmc
	./tests/test_mpmc

teste-integracao: mpmc
	python3 tests/test_integracao.py

clean:
	$(RM) mpmc tests/test_mpmc tests/mpmc_teste.o
