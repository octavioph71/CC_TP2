# Especificação dos protocolos — SubControl e EventCast

> Versão 0.2 — proposta para as fases 1 e 2 (avaliação intercalar).
> Alterações em relação à 0.1 (indicações da aula): campos minimizados; o recetor passa a
> controlar o número de sequência que espera; o publicador envia um identificador de tópico de
> 2 B em vez do tópico por extenso.
> As secções marcadas **[F3]** e **[F4]** ficam reservadas e são completadas mais tarde.

## 1. Convenções comuns

| Convenção | Valor | Porquê |
|---|---|---|
| Ordem dos bytes | *big-endian* (ordem de rede) | como no IP/TCP/UDP; em Python `struct` com prefixo `!` |
| Inteiros | sem sinal: `u8`, `u16`, `u32` | nenhum campo precisa de valores negativos |
| Cadeias (`STR8`) | `u8` comprimento + bytes UTF-8 | tópicos e ids são curtos (≤ 255 B); 1 byte de overhead |
| Tamanho dos campos | o mínimo que cobre o caso de uso | indicação da aula; cada escolha está justificada junto do campo |
| Tamanho máximo de um datagrama | 1200 B | abaixo do MTU Ethernet (1500 B) menos IP+UDP → sem fragmentação IP |
| Bits reservados | enviados a 0, ignorados na receção | permite estender sem quebrar versões anteriores |

Os cabeçalhos são construídos e interpretados campo a campo com `struct.pack`/`struct.unpack_from`.
Não se usa nenhuma biblioteca de serialização (enunciado, secção 3.3).

---

## 2. SubControl — plano de controlo, sobre TCP

### 2.1 Delimitação de mensagens

O TCP entrega uma *stream* de bytes, não mensagens (slides de Transporte). Cada mensagem SubControl
começa por um **cabeçalho fixo de 3 bytes** que diz quantos bytes de corpo se seguem:

```
 0       1       2
+-------+-------+-------+
| TYPE  |    LENGTH     |   cabeçalho fixo (3 B)
+-------+-------+-------+
|  corpo (LENGTH bytes) ...
+-----------------------
```

| Campo | Tamanho | Significado |
|---|---|---|
| `TYPE` | 1 B | tipo de mensagem (tabela 2.2) |
| `LENGTH` | 2 B | número de bytes do corpo, 0–65535 |

**Receção:** `recv_exact(3)` → lê o cabeçalho; `recv_exact(LENGTH)` → lê o corpo. `recv_exact`
repete `recv()` até ter exatamente *n* bytes, porque um `recv()` pode devolver menos do que o pedido
(ou várias mensagens coladas).

### 2.2 Tipos de mensagem

| `TYPE` | Nome | Sentido | Corpo |
|---|---|---|---|
| `0x01` | `HELLO` | cliente → broker | `ROLE u8` · `UDP_PORT u16` · `ID STR8` |
| `0x02` | `HELLO_ACK` | broker → cliente | `STATUS u8` · `DATA_PORT u16` · `SESSION u16` |
| `0x03` | `REGISTER` | publicador → broker | `QOS u8` · `TOPIC STR8` |
| `0x04` | `REGISTER_ACK` | broker → publicador | `STATUS u8` · `TOPIC_ID u16` |
| `0x05` | `SUBSCRIBE` | subscritor → broker | `REQ_ID u16` · `QOS u8` · `FILTER STR8` |
| `0x06` | `SUB_ACK` | broker → subscritor | `REQ_ID u16` · `STATUS u8` |
| `0x07` | `UNSUBSCRIBE` | subscritor → broker | `REQ_ID u16` · `FILTER STR8` |
| `0x08` | `UNSUB_ACK` | broker → subscritor | `REQ_ID u16` · `STATUS u8` |
| `0x09` | `BYE` | qualquer sentido | `REASON u8` |
| `0x10`–`0x1F` | **[F4]** reservado | broker ↔ broker | anúncio/retirada de subscrições |

**Campos:**

| Campo | Tamanho | Valores |
|---|---|---|
| `ROLE` | 1 B | `1` publicador · `2` subscritor · `3` broker **[F4]** |
| `UDP_PORT` | 2 B | porta UDP onde o cliente recebe datagramas EventCast (o cliente faz `bind` à porta 0 e anuncia a que o SO lhe deu) |
| `ID` | 1 + n B | o `--id` do processo (ex.: `pub1`) |
| `STATUS` | 1 B | `0` OK · `1` papel inválido · `2` id já em uso · `3` mensagem malformada · `4` tópico/filtro inválido · `5` tópico já registado por outro publicador · `6` filtro não subscrito |
| `DATA_PORT` | 2 B | porta UDP do plano de dados do broker — **é assim que os clientes a conhecem** (secção 5.2 do enunciado) |
| `SESSION` | 2 B | identificador da sessão, atribuído pelo broker; viaja em todos os datagramas dessa sessão e é o que diz ao broker **quem lhe está a falar** na porta UDP. 2 B chegam: até 65 535 sessões por broker, e a topologia tem 9 clientes |
| `TOPIC_ID` | 2 B | identificador do tópico, atribuído pelo broker no registo; o publicador usa-o no lugar do tópico por extenso (secção 3.2) |
| `QOS` | 1 B | `1` = G1 (at-least-once) · `2` = G2 (exactly-once) |
| `TOPIC` / `FILTER` | 1 + n B | `regiao/estacao/metrica`, ou filtro terminado em `/*` (enunciado, secção 3.4) |
| `REQ_ID` | 2 B | número do pedido, escolhido pelo subscritor, repetido na resposta |
| `REASON` | 1 B | `0` fim normal · `1` expulso (`evict_after`) · `2` erro de protocolo |

### 2.3 Trocas

**Abertura de sessão (todos os clientes).** O cliente só envia datagramas depois de receber o
`HELLO_ACK` — é isto que garante que "o broker responde antes do primeiro datagrama de cada
cliente" (fase 2).

```
cliente                                   broker
   |--- [TCP SYN / SYN-ACK / ACK] -----------|
   |--- HELLO(role, udp_port, id) ---------->|  cria sessão, atribui SESSION
   |<-- HELLO_ACK(OK, data_port, session) ---|
```

**Publicador:** um `REGISTER` por cada chave `publish`, antes de começar a contar `start_after`.
O `TOPIC_ID` recebido no `REGISTER_ACK` é o que vai nos datagramas desse fluxo.

**Subscritor:** um `SUBSCRIBE` por cada chave `subscribe`, no arranque; `SUBSCRIBE`/`UNSUBSCRIBE`
nos instantes das chaves `at` **[F4]**.

**Terminação:** quem termina envia `BYE(0)` e fecha a ligação. Na expulsão, o broker envia
`BYE(1)`, fecha a ligação TCP e descarta a fila desse subscritor.

### 2.4 Robustez

- `TYPE` desconhecido, corpo mais curto do que os campos exigem, ou `STR8` que ultrapassa o
  corpo → `BYE(2)` e fecha a ligação.
- EOF a meio de um cabeçalho ou de um corpo → a sessão é considerada terminada.
- Qualquer pedido antes do `HELLO` → `BYE(2)`.

---

## 3. EventCast — plano de dados, sobre UDP

### 3.1 Primeiro byte, comum a todos os datagramas

Para poupar um byte, o tipo e as flags partilham o primeiro byte:

```
  bit 7   6   5   4   3   2   1   0
    +---+---+---+---+---+---+---+---+
    |     TYPE      |  QOS  |RTX| 0 |
    +---+---+---+---+---+---+---+---+
```

| Campo | Bits | Significado |
|---|---|---|
| `TYPE` | 7–4 | `0x1` `PUBLISH` · `0x2` `DELIVER` · `0x3` `ACK` (até 16 tipos) |
| `QOS` | 3–2 | `1` ou `2` (0 no `ACK`) |
| `RTX` | 1 | 1 = retransmissão (informativo; ajuda a ler as capturas) |
| — | 0 | reservado |

### 3.2 `PUBLISH` (`TYPE = 0x1`) — publicador → broker

```
 0       1       2       3       4       5       6       7       8       9      10      11      12
+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
|T|Q|R  |    SESSION    |      SEQ      |            MSG_ID             |   TOPIC_ID    |    LENGTH     |
+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
|  PAYLOAD (LENGTH bytes) ...
+--------------------------
```

| Campo | Tamanho | Significado |
|---|---|---|
| 1.º byte | 1 B | `TYPE = 0x1`, `QOS`, `RTX` (secção 3.1) |
| `SESSION` | 2 B | sessão do publicador — identifica o emissor |
| `SEQ` | 2 B | número de sequência **do troço** (secção 4.3) |
| `MSG_ID` | 4 B | identificador da publicação **de ponta a ponta** |
| `TOPIC_ID` | 2 B | tópico, pelo identificador recebido no `REGISTER_ACK` |
| `LENGTH` | 2 B | comprimento do conteúdo |
| `PAYLOAD` | `LENGTH` B | conteúdo sintético, opaco para o protocolo (ex.: `23.5`) |

Cabeçalho: **13 B**. Com o tópico do enunciado e o conteúdo `23.5`: 17 B (na v0.1 eram 47 B).

**Porquê um identificador de tópico neste troço.** O tópico por extenso tem tipicamente 20–30 B e
repete-se em todas as publicações do fluxo. O publicador conhece o seu `TOPIC_ID` antes de
publicar (vem no `REGISTER_ACK`, pelo TCP), por isso não há risco de um datagrama chegar com um
identificador que o broker ainda não conhece.

### 3.3 `DELIVER` (`TYPE = 0x2`) — broker → subscritor

```
 0       1       2       3       4       5       6       7       8       9
+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
|T|Q|R  |    SESSION    |      SEQ      |            MSG_ID             |TOP_LEN|
+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
|  TOPIC (TOP_LEN bytes) ...    |    LENGTH     |  PAYLOAD (LENGTH bytes) ...
+-------------------------------+-------+-------+----------------------------
```

| Campo | Tamanho | Significado |
|---|---|---|
| 1.º byte | 1 B | `TYPE = 0x2`, `QOS` da entrega, `RTX` |
| `SESSION` | 2 B | sessão do **subscritor** (o subscritor confirma que o datagrama é para ele) |
| `SEQ` | 2 B | número de sequência deste troço, **próprio de cada subscritor** |
| `MSG_ID` | 4 B | o mesmo `MSG_ID` que veio do publicador, inalterado |
| `TOP_LEN` | 1 B | comprimento do tópico |
| `TOPIC` | `TOP_LEN` B | tópico por extenso |
| `LENGTH` | 2 B | comprimento do conteúdo |
| `PAYLOAD` | `LENGTH` B | conteúdo, copiado do `PUBLISH` |

Parte fixa: **12 B** + tópico + conteúdo.

**Porquê o tópico por extenso neste troço.** O subscritor tem de escrever o tópico no registo, e
com filtros como `north/*` não sabe de antemão que tópicos vai receber. Um identificador exigiria
anunciar cada tópico pelo SubControl antes do primeiro datagrama, e o anúncio pelo TCP pode chegar
depois do datagrama pelo UDP. Na fase 4 este formato serve também entre brokers, que encaminham
pelo tópico.

**Os dois identificadores (enunciado, secção 3.5).** `SEQ` responde a "esta confirmação é de que
transmissão?" e muda em cada troço; `MSG_ID` responde a "esta é a publicação tal?" e viaja
inalterado. No registo escreve-se o `MSG_ID` em decimal, a mesma cadeia no publicador e nos
subscritores. O `MSG_ID` mantém 4 B porque é a chave de eliminação de duplicados em G2: se desse a
volta, uma publicação nova seria confundida com uma antiga.

### 3.4 `ACK` (`TYPE = 0x3`) — recetor → emissor do troço

```
 0       1       2       3       4       5       6
+-------+-------+-------+-------+-------+-------+-------+
|T|0|0  |    SESSION    |      SEQ      |     NEXT      |
+-------+-------+-------+-------+-------+-------+-------+
```

| Campo | Tamanho | Significado |
|---|---|---|
| 1.º byte | 1 B | `TYPE = 0x3`, `QOS = 0`, `RTX = 0` |
| `SESSION` | 2 B | a mesma do datagrama confirmado |
| `SEQ` | 2 B | o `SEQ` do datagrama que provocou este `ACK` (confirmação individual) |
| `NEXT` | 2 B | o próximo `SEQ` que o recetor espera **por ordem**: todos os anteriores já chegaram (confirmação cumulativa) |

Total: **7 B**. Cada datagrama recebido é confirmado, incluindo os repetidos (se o `ACK` anterior se
perdeu, o emissor precisa de outro).

### 3.5 Validação na receção

Um datagrama é **descartado em silêncio** (sem resposta) se:

1. tiver menos de 1 B, ou `TYPE` desconhecido;
2. for mais curto do que a parte fixa do seu tipo (cabeçalho truncado);
3. o tamanho calculado a partir de `TOP_LEN` e `LENGTH` for diferente do tamanho do datagrama;
4. `SESSION` não existir ou o IP de origem não for o da sessão (no broker); ou `SESSION` não for a
   do próprio subscritor (no subscritor);
5. `TOPIC_ID` não pertencer a essa sessão (no broker);
6. `QOS` não for 1 nem 2 num `PUBLISH`/`DELIVER`.

No UDP não há a quem reclamar de forma fiável: descartar é o que o IP e o UDP fazem com um
checksum errado (slides de Transporte).

---

## 4. Mecanismo de fiabilidade (fase 2)

### 4.1 Escolha: *selective repeat*, com confirmação individual e cumulativa

- O emissor mantém até **W = 32** datagramas por confirmar, por sessão. Cada um tem o seu
  temporizador.
- Quando o temporizador de um `SEQ` expira, **só esse** é retransmitido (com `RTX = 1`), ao
  contrário do *go-back-N*, que reenviaria tudo o que vinha a seguir.
- **Por troço:** o broker confirma ao publicador assim que recebe o `PUBLISH`, e passa a ser ele o
  responsável pela entrega a cada subscritor, com uma janela, um `SEQ` e temporizadores
  **independentes por subscritor**. Um subscritor parado enche a sua janela, não a dos outros.

### 4.2 O recetor controla o número de sequência que espera

O recetor de cada troço guarda `next` (o próximo `SEQ` esperado) e o conjunto dos `SEQ` já
recebidos à frente de `next`. Quando chega um datagrama com `SEQ = s`:

| Caso | Significado | O que o recetor faz |
|---|---|---|
| `s == next` | chegou o esperado | avança `next` e continua a avançar enquanto o seguinte já estiver no conjunto |
| `s` à frente de `next` | **buraco**: algo se perdeu ou chegou fora de ordem | guarda `s` no conjunto |
| `s` atrás de `next`, ou já no conjunto | **repetido**: retransmissão cujo `ACK` se perdeu | nada no controlo de sequência |

Em todos os casos envia `ACK(SEQ = s, NEXT = next)`.

Do lado do emissor:

- Um `ACK` retira da janela o `SEQ` confirmado **e todos os anteriores a `NEXT`**. Assim, um `ACK`
  perdido fica coberto pelo seguinte.
- **Retransmissão rápida** (*fast retransmit*, como no TCP): se chegarem 3 `ACK` seguidos com o
  mesmo `NEXT` e esse `SEQ` ainda estiver por confirmar, retransmite-o logo, sem esperar pelo RTO.
- A ordem de entrega à aplicação **não** é reposta: o recetor entrega pela ordem de chegada, porque
  o enunciado não garante ordem (secção 4). `next` serve para detetar perdas e para a confirmação
  cumulativa, não para reordenar.

### 4.3 G1 não elimina duplicados

O controlo de sequência deteta que um datagrama é repetido, mas **em G1 o subscritor entrega-o na
mesma** (duas linhas no registo). O mesmo acontece no broker: um `PUBLISH` retransmitido pelo
publicador é reencaminhado outra vez. É o comportamento exigido: "uma publicação que chegue duas
vezes fica registada duas vezes".

**[F3]** Em G2, antes de escrever no registo, o subscritor consulta uma tabela de duplicados
indexada por `(TOPIC, MSG_ID)`. Continua a confirmar o repetido, mas não o volta a entregar.

### 4.4 Números de sequência e volta (*wrap-around*)

- `SEQ` é um `u16` que começa em 0 em cada sessão e incrementa 1 por datagrama novo, módulo 2¹⁶.
  As retransmissões usam o mesmo `SEQ`.
- As comparações "à frente/atrás" usam **aritmética de números de série** (RFC 1982):
  `a` está à frente de `b` se `(a − b) mod 2¹⁶` estiver entre 1 e 2¹⁵ − 1.
- Isto só é inequívoco porque a janela (32) é muito menor do que metade do espaço (32 768): nunca
  há dois datagramas em trânsito com o mesmo `SEQ`. A 10 publicações/s, o contador dá a volta ao
  fim de cerca de 1 h 49 min.
- `MSG_ID` é um `u32` por fluxo, de 1 a `count`.

### 4.5 Temporizadores

| Temporizador | Onde | Valor | Ao expirar |
|---|---|---|---|
| **RTO** (retransmissão) | emissor de cada troço, por `SEQ` | inicial 300 ms; depois `EstimatedRTT + 4·DevRTT` (α = 0,125, β = 0,25), limitado a [50 ms, 2 s] | retransmite esse datagrama com `RTX = 1` e duplica o RTO desse `SEQ` (*backoff*), até ao limite de 2 s |
| Amostra de RTT | emissor | medida do envio ao `ACK` | só de datagramas **não retransmitidos** (algoritmo de Karn: com retransmissões não se sabe a que envio o `ACK` responde) |
| `start_after` | publicador | configuração | começa a publicar (conta a partir do `HELLO_ACK` e dos `REGISTER_ACK`) |
| Ritmo de publicação | publicador, por fluxo | `1 / rate` s | publica a próxima mensagem do fluxo |
| `drain` | publicador | configuração | depois da última publicação: termina quando tudo estiver confirmado ou quando `drain` expirar, o que vier primeiro; envia `BYE(0)` |
| `evict_after` | broker, por subscritor | configuração | expulsa: `BYE(1)`, fecha o TCP, descarta a fila |
| `duration` | broker, subscritor | configuração | termina; o subscritor envia `BYE(0)` antes |

**Contagem do `evict_after`.** O broker guarda, por subscritor, `silent_since`: o instante a
partir do qual há algo por confirmar sem que tenha chegado nenhum `ACK`. Atualiza-o quando a fila
passa de vazia a não vazia e sempre que chega um `ACK`. Um ciclo de verificação a cada 100 ms
expulsa o subscritor se `pendentes > 0` e `agora − silent_since ≥ evict_after`. A expulsão
acontece portanto entre `evict_after` e `evict_after + 0,1 s`: "nem antes nem muito depois".

---

## 5. *Dry run* — fase 2, com perdas

Configuração: `pub1` publica `north/station7/temperature qos=1`; `sub1` subscreve
`north/station7/* qos=1`. Broker com `data_port = 7311`.

```
pub1 (10.0.1.10)                     broker1 (10.0.3.20)                      sub1 (10.0.2.31)
     |                                       |                                       |
     |                                       |<-- HELLO(role=2, udp=40002, "sub1") --|
     |                                       |--- HELLO_ACK(0, 7311, sess=2) ------->|
     |                                       |<-- SUBSCRIBE(req=1, qos=1, "north/station7/*")
     |                                       |--- SUB_ACK(req=1, 0) ---------------->|
     |-- HELLO(role=1, udp=40001, "pub1") -->|                                       |
     |<- HELLO_ACK(0, 7311, sess=1) ---------|                                       |
     |-- REGISTER(qos=1, "north/station7/temperature") -->|                          |
     |<- REGISTER_ACK(0, topic_id=1) --------|                                       |
     |   ... start_after ...                 |                                       |
     |-- PUBLISH(sess=1, seq=0, msg=1, tid=1) ->|                                    |
     |<- ACK(sess=1, seq=0, next=1) ---------|-- DELIVER(sess=2, seq=0, msg=1) ----->|  regista msg 1
     |                                       |<- ACK(sess=2, seq=0, next=1) ---------|
     |-- PUBLISH(sess=1, seq=1, msg=2) ----->|                                       |
     |<- ACK(sess=1, seq=1, next=2) ---------|-- DELIVER(sess=2, seq=1, msg=2) --X   |  perdido
     |-- PUBLISH(sess=1, seq=2, msg=3) ----->|                                       |
     |<- ACK(sess=1, seq=2, next=3) ---------|-- DELIVER(sess=2, seq=2, msg=3) ----->|  buraco em 1; regista msg 3
     |                                       |<- ACK(sess=2, seq=2, next=1) ---------|  NEXT continua em 1
     |                                       |   ... RTO de seq=1 expira ...         |
     |                                       |-- DELIVER(sess=2, seq=1, msg=2, RTX) ->|  regista msg 2
     |                                       |<- ACK(sess=2, seq=1, next=3) ---------|  buraco fechado
     |-- PUBLISH(sess=1, seq=3, msg=4) ----->|                                       |
     |      X-- ACK(sess=1, seq=3, next=4) --|-- DELIVER(sess=2, seq=3, msg=4) ----->|  regista msg 4
     |   ... RTO de seq=3 expira ...         |<- ACK(sess=2, seq=3, next=4) ---------|
     |-- PUBLISH(sess=1, seq=3, msg=4, RTX) ->|  seq=3 já recebido: repetido         |
     |<- ACK(sess=1, seq=3, next=4) ---------|-- DELIVER(sess=2, seq=4, msg=4) ----->|  regista msg 4 (G1: duplicado)
     |                                       |<- ACK(sess=2, seq=4, next=5) ---------|
```

Notas:

- **Perda de um `DELIVER` (seq=1).** O subscritor deteta o buraco quando chega o seq=2
  (`NEXT` fica em 1) e entrega a msg 3 antes da msg 2: a ordem não é garantida. Com mais
  datagramas em trânsito, três `ACK` com `NEXT = 1` fariam o broker retransmitir antes do RTO.
- **Perda de um `ACK` para o publicador (seq=3).** O publicador retransmite. O broker vê que o
  seq=3 é repetido mas, em G1, reencaminha-o: o `DELIVER` leva um **`SEQ` novo (4)** e o
  **mesmo `MSG_ID` (4)**, e o subscritor regista a msg 4 duas vezes. Em G2 **[F3]**, `(tópico, 4)`
  seria detetado como repetido.

### 5.1 Exemplos em bytes

`HELLO` de `pub1` (SubControl), 11 B:

```
01 00 08 | 01 | 9C 41 | 04 70 75 62 31
TYPE LENGTH=8 | ROLE=1 | UDP_PORT=40001 | ID="pub1"
```

`PUBLISH` (pub1 → broker): sessão 1, `SEQ` 0, `MSG_ID` 1, `qos=1`, `TOPIC_ID` 1, conteúdo
`23.5`, **17 B**:

```
14 | 00 01 | 00 00 | 00 00 00 01 | 00 01 | 00 04 | 32 33 2E 35
TYPE=1,QOS=1 | SESSION=1 | SEQ=0 | MSG_ID=1 | TOPIC_ID=1 | LENGTH=4 | "23.5"
```

`DELIVER` (broker → sub1): sessão 2, `SEQ` 0, `MSG_ID` 1, tópico `north/station7/temperature`
(26 B), **42 B**:

```
24 | 00 02 | 00 00 | 00 00 00 01 | 1A | 6E 6F 72 74 68 2F ... 65 | 00 04 | 32 33 2E 35
TYPE=2,QOS=1 | SESSION=2 | SEQ=0 | MSG_ID=1 | TOP_LEN=26 | TOPIC | LENGTH=4 | "23.5"
```

`ACK` (broker → pub1) do `PUBLISH` acima, **7 B**: `30 | 00 01 | 00 00 | 00 01`
(`TYPE=3`, `SESSION=1`, `SEQ=0`, `NEXT=1`).

---

## 6. Estado que cada entidade mantém

| Entidade | Estado |
|---|---|
| **Broker** | sessões (`SESSION` → papel, id, IP, `UDP_PORT`, socket TCP); tópicos registados (`TOPIC_ID` → tópico, sessão do publicador, qos); subscrições (subscritor → filtros e qos); **por publicador** (recetor): `next` e conjunto dos `SEQ` à frente; **por subscritor** (emissor): próximo `SEQ`, datagramas em trânsito (`SEQ` → datagrama, instante de envio, RTO, retransmitido?), contador de `ACK` com o mesmo `NEXT`, fila de espera quando a janela está cheia, `silent_since`, estimativas de RTT |
| **Publicador** | `SESSION`, porta de dados do broker; por fluxo: `TOPIC_ID`, próximo `MSG_ID`, instante da próxima publicação; próximo `SEQ`; datagramas em trânsito e contador de `ACK` com o mesmo `NEXT`; estimativas de RTT |
| **Subscritor** | `SESSION`; filtros subscritos; `next` e conjunto dos `SEQ` à frente; **[F3]** tabela de duplicados `(TOPIC, MSG_ID)` para entregas G2 |

---

## 7. Decisões em aberto

- **[F3]** Onde se decide a cópia única com filtros sobrepostos. Proposta: no broker, para seguir
  um só datagrama (poupa rede, vê-se na captura).
- **[F3]** Eliminação de duplicados em G2: só no subscritor, ou também no broker (para não
  reencaminhar um `PUBLISH` que o publicador retransmitiu).
- **[F4]** Mensagens SubControl entre brokers (`0x10`–`0x1F`), flooding vs. propagação de
  subscrições, e agregação de filtros (*route summarization*).
- O conjunto dos `SEQ` à frente de `next` pode crescer se o emissor nunca preencher um buraco
  (por exemplo, depois de uma expulsão). Limitá-lo à janela (32) e descartar o que fique para trás.
