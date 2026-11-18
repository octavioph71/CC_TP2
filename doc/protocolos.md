# Especificação dos protocolos

SubControl (TCP, controlo) e EventCast (UDP, dados). Todos os campos são inteiros sem sinal em
*network byte order* (big-endian), construídos e lidos byte a byte com `struct`. As fases 2 a 4
acrescentam mensagens a estas; os bits e tipos reservados estão indicados.

## 1. SubControl — sobre TCP
O TCP é uma stream de bytes, por isso cada mensagem leva um cabeçalho de 3 bytes e é lida em dois
passos: `recv_exact(3)` e depois `recv_exact(LENGTH)`.

```
+------+-----------+------------------+
| TYPE |  LENGTH   | corpo (LENGTH B) |      TYPE u8 · LENGTH u16
+------+-----------+------------------+
```

| TYPE | Mensagem | Sentido | Corpo |
|---|---|---|---|
| `0x01` | `HELLO` | cliente → broker | `ROLE u8` · `UDP_PORT u16` · `ID str8` |
| `0x02` | `HELLO_ACK` | broker → cliente | `STATUS u8` · `DATA_PORT u16` · `SESSION u16` |
| `0x03` | `REGISTER` | pub → broker | `QOS u8` · `TOPIC str8` |
| `0x04` | `REGISTER_ACK` | broker → pub | `STATUS u8` · `TOPIC_ID u16` |
| `0x05` | `SUBSCRIBE` | sub → broker | `QOS u8` · `FILTER str8` |
| `0x06` | `SUB_ACK` | broker → sub | `STATUS u8` |
| `0x09` | `BYE` | qualquer | `REASON u8` (0 normal · 2 erro de protocolo) |

`str8` = comprimento `u8` + bytes UTF-8. `ROLE`: 1 publicador, 2 subscritor.
`STATUS`: 0 ok  · 2 id em uso · 3 qos inválido · 4 tópico/filtro inválido · 5 tópico já pertence a outro
publicador (o 1 fica livre). Reservados: `0x07`/`0x08` (UNSUBSCRIBE, fase 4), `0x10–0x1F` (brokers).

**Quem fala com o broker.** O `HELLO` traz o `--id` e a porta UDP do cliente. O broker responde com
um `SESSION` de 2 B único, e é esse número que o cliente põe em cada datagrama UDP: um datagrama
sem `SESSION` conhecido, ou vindo de um IP diferente do da sessão, é descartado. O `HELLO_ACK` traz
também a `DATA_PORT` (os clientes só conhecem a porta de controlo).

**Troca de abertura.** O cliente só envia UDP depois do `HELLO_ACK`:

```
cliente                                  broker
   |--- TCP SYN / SYN-ACK / ACK ---------->|
   |--- HELLO(role, udp_port, "pub1") ---->|   cria a sessão
   |<-- HELLO_ACK(0, data_port, session) --|
   |--- REGISTER / SUBSCRIBE ------------->|   (um por chave publish / subscribe)
   |<-- REGISTER_ACK(topic_id) / SUB_ACK --|
```

**Robustez** (o que pode correr mal): o cliente repete o `connect` a cada 200 ms até 10 s (o broker
pode ainda não ter arrancado) e desiste se o `HELLO` não tiver resposta em 5 s; o broker larga quem
abre o TCP e não envia `HELLO` em 5 s; tipo desconhecido, corpo com tamanho errado, `UTF-8`
inválido ou um pedido que o papel não pode fazer → `BYE(2)` e fecha-se a ligação; EOF a meio de
uma mensagem → sessão terminada; `HELLO` com papel desconhecido, porta 0 ou id vazio → `BYE(2)`;
id repetido → `HELLO_ACK(2)`.

## 2. EventCast — sobre UDP

Primeiro byte comum (o tipo e a garantia partilham um byte, para poupar bytes):

```
 bit  7  6  5  4   3    2  1  0
     +-----------+----+---------+
     |   TYPE    | Q2 | (0 0 0) |     Q2 = 0 → qos 1 (G1) · Q2 = 1 → qos 2 (G2) · bits 2–0 reservados
     +-----------+----+---------+
```

`TYPE`: `0x1` PUBLISH · `0x2` DELIVER · `0x3` ACK. O formato ACK está implementado, mas o envio, a receção funcional e as retransmissões ainda não estão integrados.

**PUBLISH** (publicador → broker), cabeçalho de **11 B**:

| Campo | B | Significado |
|---|---|---|
| 1.º byte | 1 | `TYPE=1`, `Q2` |
| `SESSION` | 2 | sessão do publicador (identifica quem fala) |
| `SEQ` | 2 | nº de sequência deste troço; começa em 0 e dá a volta (módulo 2¹⁶) |
| `MSG_ID` | 4 | identifica a publicação de ponta a ponta; 1 a `count` por fluxo |
| `TOPIC_ID` | 2 | tópico, o recebido no `REGISTER_ACK` (evita repetir 25 B de tópico em cada datagrama) |
| conteúdo | resto | opaco; o tamanho é o do datagrama menos 11 B (sem campo `LENGTH`) |

**DELIVER** (broker → subscritor), cabeçalho de **10 B** + tópico:

| Campo | B | Significado |
|---|---|---|
| 1.º byte | 1 | `TYPE=2`, `Q2` da entrega |
| `SESSION` | 2 | sessão do **subscritor** |
| `SEQ` | 2 | nº de sequência deste troço, próprio de cada subscritor |
| `MSG_ID` | 4 | o mesmo do PUBLISH, inalterado |
| `TOP_LEN` | 1 | comprimento do tópico |
| `TOPIC` | `TOP_LEN` | tópico por extenso (o subscritor escreve-o no registo e não sabe de antemão os tópicos de `north/*`) |
| conteúdo | resto | copiado do PUBLISH |

**ACK** (recetor → emissor do troço), cabeçalho fixo de **7 B**:

| Campo | B | Significado |
|---|---|---|
| 1.º byte | 1 | `TYPE=3`, sem bit Q2 |
| `SESSION` | 2 | sessão a que respeita a confirmação |
| `SEQ` | 2 | sequência da transmissão confirmada |
| `NEXT` | 2 | primeira sequência ainda não recebida em ordem |

**Estado:** `eventcast.py` já codifica e descodifica ACK, mas os processos ainda não os enviam, processam nem guardam retransmissões.

`SEQ` (âmbito de um troço) e `MSG_ID` (âmbito de ponta a ponta) são campos diferentes: o primeiro
serve para detetar perdas e ordem, o segundo para identificar a publicação.

**Validação.** Um datagrama é descartado em silêncio se: estiver vazio ou tiver `TYPE`
desconhecido; for mais curto do que o cabeçalho do seu tipo; no DELIVER, o `TOP_LEN` passar o fim
do datagrama; o `SESSION` não existir ou o IP de origem não for o da sessão (broker) / o `SESSION` não for o
do subscritor; o `TOPIC_ID` não for do publicador.

## 3. Controlo do número de sequência

O recetor de cada troço (o broker para cada publicador, cada subscritor para o broker) guarda
`next`, o `SEQ` que espera, e o conjunto dos `SEQ` que chegaram à frente. Ao chegar `s`:

| Caso | Ação |
|---|---|
| `s == next` | aceita; avança `next` (e continua enquanto o seguinte já estiver guardado) |
| `s` até 31 posições à frente de `next`, ainda não visto | aceita (**buraco ou fora de ordem**); guarda `s`, `next` não avança |
| qualquer outro: já visto, já passado, ou a 32 ou mais à frente | descartado |

A distância é `(s − next) mod 2¹⁶`, por isso `65535 → 0` não é uma perda. Nesta fase o recetor
regista o buraco (`esperava SEQ X, chegou Y`) e entrega pela ordem de chegada; a recuperação (ACK e
retransmissão) é a fase 2.

## 4. Estado em cada processo

| Processo | Estado |
|---|---|
| Broker | `SessionStore` (`broker/sessions.py`): `sessions` (SESSION → papel, id, IP, porta UDP, socket TCP) · `topic_owner` (um publicador por tópico) · por publicador: `TOPIC_ID → (tópico, qos)` e o `next` esperado · `SubscriptionTable` (filtro → {sessão → qos}) · por subscritor: próximo `SEQ` |
| Publicador | `SESSION`, endereço de dados do broker, `TOPIC_ID` de cada fluxo, próximo `SEQ` |
| Subscritor | `SESSION`, `next` esperado |

**Tabela de subscrições.** Um tópico `r/s/m` só pode ser correspondido por 3 filtros (`r/s/m`,
`r/s/*`, `r/*`), por isso encontrar os interessados são 3 consultas a um dicionário. Uma sessão
aparece uma só vez, mesmo com filtros sobrepostos, com o `qos` mais forte; a entrega usa o mais
fraco entre esse e o do fluxo. A cópia única é decidida no broker: segue um só datagrama.

## 5. Exemplo (dry run, sem perdas)

`pub1` publica `north/station7/temperature qos=1`; `sub1` subscreve `north/station7/*`.

```
pub1                                  broker1                               sub1
  |                                      |<-- HELLO(2, 40002, "sub1") ---------|
  |                                      |--- HELLO_ACK(0, 7311, 1) ---------->|
  |                                      |<-- SUBSCRIBE(1, "north/station7/*") |
  |                                      |--- SUB_ACK(0) --------------------->|
  |-- HELLO(1, 40001, "pub1") ---------->|                                     |
  |<- HELLO_ACK(0, 7311, 2) -------------|                                     |
  |-- REGISTER(1, "north/station7/temperature") ->|                            |
  |<- REGISTER_ACK(0, 1) ----------------|                                     |
  |-- PUBLISH(sess=2, seq=0, msg=1, tid=1) ->| casa com north/station7/*        |
  |                                      |-- DELIVER(sess=1, seq=0, msg=1) --->|  regista msg 1
  |-- PUBLISH(sess=2, seq=1, msg=2) ---->|                                     |
  |                                      |-- DELIVER(sess=1, seq=1, msg=2) --->|  regista msg 2
```

Em bytes — `HELLO` de `pub1` (11 B): `01 00 08 | 01 | 9C 41 | 04 70 75 62 31`
(`TYPE`, `LENGTH=8`, `ROLE=1`, `UDP_PORT=40001`, `"pub1"`).
`PUBLISH` (15 B): `10 | 00 02 | 00 00 | 00 00 00 01 | 00 01 | 32 33 2E 35`
(`TYPE=1,Q2=0`, `SESSION=2`, `SEQ=0`, `MSG_ID=1`, `TOPIC_ID=1`, `"23.5"`).

## 6. Fica para as fases seguintes

Fase 2: integrar o ACK já codificado, retransmissão e temporizadores, expulsão por `evict_after`. Fase 3: eliminação de
duplicados em G2. Fase 4: `UNSUBSCRIBE`, ações `at` e brokers vizinhos.