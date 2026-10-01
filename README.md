# mpmc: Múltiplos Produtores / Múltiplos Consumidores

Trabalho Prático 02/2026 da disciplina de Sistemas Operacionais (UnB, Prof.ª Alba Melo).

Sistema de comunicação entre processos produtores e consumidores em C, usando IPC System V no Linux. Implementa duas abordagens:

| Paradigma | Valor | Mecanismos IPC |
|-----------|:-----:|----------------|
| Memória compartilhada | `1` | `shm` + `sem` |
| Troca de mensagens    | `2` | `msg` |

Sinais Unix também podem ser usados.

## Estrutura

```
.
├── mpmc.c      # inicializa o sistema (IPC + produtores)
├── produz.c    # produz mensagens
├── consome.c   # consome mensagens
└── utils.h     # constantes e definições compartilhadas
```

## Build

Requer GCC e Linux.

```bash
gcc -Wall -o mpmc    mpmc.c
gcc -Wall -o produz  produz.c
gcc -Wall -o consome consome.c
```

## Uso

### mpmc

Cria os mecanismos IPC e os `p` produtores.

```bash
./mpmc <p> <paradigma> &
```

| Argumento   | Descrição |
|-------------|-----------|
| `p`         | número de produtores |
| `paradigma` | `1` memória compartilhada, `2` troca de mensagens |

### produz

```bash
./produz <p> <n_msgs> <ack> &
```

| Argumento | Descrição |
|-----------|-----------|
| `p`       | produtor que envia. Com `0`, as mensagens são distribuídas no padrão striped (`1, 2, ..., p, 1, 2, ..., p`) |
| `n_msgs`  | número de mensagens (1 a 1024) |
| `ack`     | `1` espera confirmação de que tudo foi consumido, `2` não espera |

Se `p` não existir, retorna erro.

### consome

```bash
./consome <p> <n_msgs> <ack> &
```

| Argumento | Descrição |
|-----------|-----------|
| `p`       | produtor de onde consumir |
| `n_msgs`  | número de mensagens a consumir (1 a 1024) |
| `ack`     | se faltarem mensagens: `1` bloqueia, `2` retorna erro informando quantas foram consumidas |

### shutdown

```bash
./shutdown
```

Termina produtores e consumidores, imprime o tempo de execução de cada processo e o número de mensagens consumidas, e remove os mecanismos IPC.

## Exemplos

### 4 produtores, memória compartilhada

```bash
./mpmc 4 1 &
./produz 3 2 1 &      # produz 2 msgs e bloqueia aguardando consumo
./consome 2 2 1 &     # bloqueia até haver 2 msgs do produtor 2
./produz 2 5 1 &      # produz 5 msgs; o consumidor consome 2
./shutdown            # encerra com produtor ainda bloqueado
```

### 2 produtores, troca de mensagens

```bash
./mpmc 2 2 &
./produz 3 2 1 &      # erro: produtor 3 não existe
./consome 2 2 1 &     # bloqueia
./consome 2 2 1 &     # bloqueia
./produz 2 5 2 &      # produz 5 msgs sem bloquear; cada consumidor leva 2 e termina
./shutdown
```

### 5 produtores, troca de mensagens (striped)

```bash
./mpmc 5 2 &
./produz 0 15 2 &     # 15 msgs distribuídas em 5 filas (3 em cada)
./consome 2 3 1 &
./consome 1 2 1 &
./produz 3 2 1 &      # produz 2 msgs e bloqueia
./shutdown
```

## Limpeza de recursos IPC

Se o programa morrer sem passar pelo `shutdown`, os recursos ficam alocados e o `mpmc` falha com `shmget: File exists`.

```bash
ipcs              # lista shm, sem e msg ativos
ipcrm -M 0x1234   # remove a shm pela chave
ipcrm -a          # remove todos os recursos IPC do usuário
```

## Status

- [x] Validação de argumentos do `mpmc`
- [x] Criação e inicialização da memória compartilhada
- [ ] Criação dos produtores (`fork`)
- [ ] Sincronização com semáforos
- [ ] Paradigma de troca de mensagens
- [ ] `produz`
- [ ] `consome`
- [ ] `shutdown`
