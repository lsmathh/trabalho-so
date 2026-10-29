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
