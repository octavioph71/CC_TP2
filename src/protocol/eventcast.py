"""
EventCast — plano de dados, sobre UDP (docs/protocolos.md, secção 3).

Primeiro byte, comum a todos os datagramas:

      bit 7   6   5   4   3   2   1   0
        +---+---+---+---+---+---+---+---+
        |     TYPE      |  QOS  |RTX| 0 |
        +---+---+---+---+---+---+---+---+

    PUBLISH (pub -> broker), 13 B + conteúdo:
        1.º byte · SESSION u16 · SEQ u16 · MSG_ID u32 · TOPIC_ID u16 · LENGTH u16 · PAYLOAD

    DELIVER (broker -> sub), 12 B + tópico + conteúdo:
        1.º byte · SESSION u16 · SEQ u16 · MSG_ID u32 · TOP_LEN u8 · TOPIC · LENGTH u16 · PAYLOAD

    ACK (recetor -> emissor do troço), 7 B:
        1.º byte · SESSION u16 · SEQ u16 · NEXT u16

Este módulo só trata de bytes. Verifica a estrutura de cada datagrama
(regras 1, 2, 3 e 6 da secção 3.5); se a SESSION e o TOPIC_ID existem
(regras 4 e 5) é verificado por quem recebe, que conhece as sessões.

Uso típico:
    data = encode(Publish(session=1, seq=0, msg_id=1, topic_id=1, qos=1, payload=b"23.5"))
    sock.sendto(data, (broker_ip, data_port))

    data, addr = sock.recvfrom(RECV_BUFSIZE)
    try:
        msg = decode(data)
    except DecodeError:
        pass                            # descarta em silêncio
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Union

# --------------------------------------------------------------------------- #
#  Constantes
# --------------------------------------------------------------------------- #

T_PUBLISH = 0x1
T_DELIVER = 0x2
T_ACK     = 0x3

RTX_BIT = 0x02                   # bit 1 do primeiro byte

MAX_DATAGRAM = 1200              # abaixo do MTU Ethernet: sem fragmentação IP
RECV_BUFSIZE = 65535             # buffer de receção: maior do que qualquer datagrama UDP,
                                 # para um datagrama grande não ser cortado sem darmos conta

# Partes fixas de cada formato (formato do struct, sem o conteúdo nem o tópico)
_PUBLISH_HDR = struct.Struct("!BHHIHH")    # 1+2+2+4+2+2 = 13 B
_DELIVER_HDR = struct.Struct("!BHHIB")     # 1+2+2+4+1   = 10 B (+ tópico + LENGTH + conteúdo)
_LENGTH      = struct.Struct("!H")         # 2 B
_ACK         = struct.Struct("!BHHH")      # 1+2+2+2     =  7 B

PUBLISH_HDR_SIZE = _PUBLISH_HDR.size       # 13
DELIVER_MIN_SIZE = _DELIVER_HDR.size + _LENGTH.size   # 12 (tópico vazio, sem conteúdo)
ACK_SIZE = _ACK.size                       # 7


class DecodeError(Exception):
    """Datagrama inválido. Quem recebe descarta-o em silêncio."""


# --------------------------------------------------------------------------- #
#  Mensagens
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Publish:          # publicador -> broker
    session: int
    seq: int
    msg_id: int
    topic_id: int
    qos: int
    payload: bytes
    retx: bool = False


@dataclass(frozen=True)
class Deliver:          # broker -> subscritor
    session: int
    seq: int
    msg_id: int
    topic: str
    qos: int
    payload: bytes
    retx: bool = False


@dataclass(frozen=True)
class Ack:              # recetor -> emissor do troço
    session: int
    seq: int
    next: int


Datagram = Union[Publish, Deliver, Ack]


# --------------------------------------------------------------------------- #
#  Primeiro byte
# --------------------------------------------------------------------------- #

def _first_byte(msg_type: int, qos: int, retx: bool) -> int:
    return (msg_type << 4) | (qos << 2) | (RTX_BIT if retx else 0)


def _split_first_byte(b: int):
    """-> (TYPE, QOS, RTX). O bit 0 (reservado) é ignorado."""
    return b >> 4, (b >> 2) & 0x3, bool(b & RTX_BIT)


def mark_retx(data: bytes) -> bytes:
    """
    Devolve o mesmo datagrama com RTX = 1. O emissor guarda os bytes do envio
    original e, numa retransmissão, só muda este bit — sem voltar a construir tudo.
    """
    return bytes([data[0] | RTX_BIT]) + data[1:]


def _check_qos(qos: int) -> None:
    if qos not in (1, 2):
        raise ValueError(f"qos tem de ser 1 ou 2: {qos}")


# --------------------------------------------------------------------------- #
#  Construir (encode)
# --------------------------------------------------------------------------- #

def encode(msg: Datagram) -> bytes:
    if isinstance(msg, Publish):
        _check_qos(msg.qos)
        data = _PUBLISH_HDR.pack(
            _first_byte(T_PUBLISH, msg.qos, msg.retx),
            msg.session, msg.seq, msg.msg_id, msg.topic_id, len(msg.payload),
        ) + msg.payload

    elif isinstance(msg, Deliver):
        _check_qos(msg.qos)
        topic = msg.topic.encode("utf-8")
        if len(topic) > 0xFF:
            raise ValueError(f"tópico com mais de 255 bytes: {msg.topic[:20]}...")
        data = (
            _DELIVER_HDR.pack(
                _first_byte(T_DELIVER, msg.qos, msg.retx),
                msg.session, msg.seq, msg.msg_id, len(topic),
            )
            + topic
            + _LENGTH.pack(len(msg.payload))
            + msg.payload
        )

    elif isinstance(msg, Ack):
        data = _ACK.pack(_first_byte(T_ACK, 0, False), msg.session, msg.seq, msg.next)

    else:
        raise TypeError(f"não é um datagrama EventCast: {msg!r}")

    if len(data) > MAX_DATAGRAM:
        raise ValueError(f"datagrama com {len(data)} B excede {MAX_DATAGRAM} B")
    return data


# --------------------------------------------------------------------------- #
#  Interpretar (decode) e validar
# --------------------------------------------------------------------------- #

def decode(data: bytes) -> Datagram:
    """
    Interpreta um datagrama recebido. Lança DecodeError se:
      1. tiver menos de 1 B, ou TYPE desconhecido;
      2. for mais curto do que a parte fixa do seu tipo (cabeçalho truncado);
      3. o tamanho calculado (TOP_LEN, LENGTH) não for o tamanho do datagrama;
      6. QOS não for 1 nem 2 num PUBLISH/DELIVER.
    """
    if not data:
        raise DecodeError("datagrama vazio")

    msg_type, qos, retx = _split_first_byte(data[0])

    if msg_type == T_PUBLISH:
        if len(data) < PUBLISH_HDR_SIZE:
            raise DecodeError(f"PUBLISH truncado: {len(data)} B < {PUBLISH_HDR_SIZE} B")
        _, session, seq, msg_id, topic_id, length = _PUBLISH_HDR.unpack_from(data, 0)
        if len(data) != PUBLISH_HDR_SIZE + length:
            raise DecodeError(
                f"PUBLISH com {len(data)} B, LENGTH indica {PUBLISH_HDR_SIZE + length} B")
        _require_qos(qos)
        return Publish(session, seq, msg_id, topic_id, qos,
                       bytes(data[PUBLISH_HDR_SIZE:]), retx)

    if msg_type == T_DELIVER:
        if len(data) < DELIVER_MIN_SIZE:
            raise DecodeError(f"DELIVER truncado: {len(data)} B < {DELIVER_MIN_SIZE} B")
        _, session, seq, msg_id, top_len = _DELIVER_HDR.unpack_from(data, 0)

        topic_start = _DELIVER_HDR.size
        len_pos = topic_start + top_len
        if len(data) < len_pos + _LENGTH.size:
            raise DecodeError(f"DELIVER truncado: TOP_LEN={top_len} passa o fim do datagrama")
        (length,) = _LENGTH.unpack_from(data, len_pos)

        payload_start = len_pos + _LENGTH.size
        if len(data) != payload_start + length:
            raise DecodeError(
                f"DELIVER com {len(data)} B, TOP_LEN/LENGTH indicam {payload_start + length} B")
        _require_qos(qos)
        try:
            topic = bytes(data[topic_start:len_pos]).decode("utf-8")
        except UnicodeDecodeError:
            raise DecodeError("tópico com UTF-8 inválido") from None
        return Deliver(session, seq, msg_id, topic, qos,
                       bytes(data[payload_start:]), retx)

    if msg_type == T_ACK:
        if len(data) != ACK_SIZE:
            raise DecodeError(f"ACK com {len(data)} B (esperava {ACK_SIZE} B)")
        _, session, seq, nxt = _ACK.unpack(data)
        return Ack(session, seq, nxt)

    raise DecodeError(f"TYPE desconhecido: 0x{msg_type:x}")


def _require_qos(qos: int) -> None:
    if qos not in (1, 2):
        raise DecodeError(f"QOS inválido: {qos}")