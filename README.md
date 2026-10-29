# Produtores e consumidores

Trabalho de Sistemas Operacionais para implementar a comunicação entre múltiplos
produtores e consumidores em C, usando memória compartilhada e troca de mensagens.

## Como rodar

Compile e execute:

```bash
make
./mpmc 4 1
```

O primeiro argumento é a quantidade de produtores. O segundo é o paradigma:
`1` para memória compartilhada e `2` para troca de mensagens (ainda não implementado).

Exemplo de comandos dentro do programa:

```text
produz 1 2 1
consome 1 2 1
shutdown
```

Os argumentos são: produtor, quantidade de mensagens e ACK. Com ACK `1`, o
produtor espera a confirmação do consumo e o consumidor espera mensagens se
faltarem. Com `2`, não há essas esperas. Use produtor `0` no comando `produz`
para distribuir as mensagens entre todas as filas.

## TODO

- Medir o tempo de cada produtor, consumidor e do mpmc.
- Guardar os tempos dos processos que terminarem antes do shutdown.
- Imprimir no shutdown o tempo de cada PID, incluindo os encerrados enquanto bloqueados.
- Imprimir o total de mensagens consumidas em cada fila antes de remover os segmentos.
- Implementar o paradigma 2 com `msgget`, `msgsnd` e `msgrcv`, incluindo a limpeza das filas.
