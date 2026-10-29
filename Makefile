CC = gcc
CFLAGS = -Wall -Wextra -g
FONTES = mpmc.c utils.c
CABECALHOS = utils.h

.PHONY: all clean calcula

all: mpmc

mpmc: $(FONTES) $(CABECALHOS)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $(FONTES) $(LDLIBS) -o $@

clean:
	$(RM) mpmc
