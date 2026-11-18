"""
EventCast — plano de dados, sobre UDP (doc/protocolos.md, secção 3).
A estrutura ACK foi reposta para a Fase 2, mas ainda não é utilizada pelos processos.

Primeiro byte, comum aos datagramas:

      bit 7   6   5   4   3   2   1   0
        +---+---+---+---+---+---+---+---+
        |     TYPE      |Q2 | reservados|     Q2 = 0 -> qos 1 · Q2 = 1 -> qos 2
        +---+---+---+---+---+---+---+---+

    PUBLISH (pub -> broker), 11 B + conteúdo:
        1.º byte · SESSION u16 · SEQ u16 · MSG_ID u32 · TOPIC_ID u16 · PAYLOAD
    DELIVER (broker -> sub), 10 B + tópico + conteúdo:
        1.º byte · SESSION u16 · SEQ u16 · MSG_ID u32 · TOP_LEN u8 · TOPIC · PAYLOAD
    ACK (recetor -> emissor), 7 B:
        1.º byte · SESSION u16 · SEQ u16 · NEXT u16

Não há campo LENGTH: o UDP preserva os limites do datagrama, por isso o conteúdo é o que
sobra depois da parte fixa. Um datagrama inválido lança DecodeError e quem recebe descarta-o.
"""

import struct
from dataclasses import dataclass
from typing import Union

T_PUBLISH, T_DELIVER, T_ACK = 0x1, 0x2, 0x3
Q2_BIT = 0x08

MAX_DATAGRAM = 1200            
RECV_BUFSIZE = 65535

_PUBLISH_HDR = struct.Struct("!BHHIH")     # 11 Bytes
_DELIVER_HDR = struct.Struct("!BHHIB")     # 10 Bytes
_ACK_HDR = struct.Struct("!BHHH")          # 7 Bytes


class DecodeError(Exception):
    pass


@dataclass(frozen=True)
class Publish:         
    session: int
    seq: int
    msg_id: int
    topic_id: int
    qos: int
    payload: bytes

@dataclass(frozen=True)
class Deliver:         
    session: int
    seq: int
    msg_id: int
    topic: str
    qos: int
    payload: bytes


@dataclass(frozen=True)
class Ack:
    session: int
    seq: int
    next: int


Datagram = Union[Publish, Deliver, Ack]

def _first_byte(msg_type: int, qos: int) -> int:
    if qos not in (1, 2):
        raise ValueError(f"qos tem de ser 1 ou 2: {qos}")
    return (msg_type << 4) | (Q2_BIT if qos == 2 else 0)


def encode(msg: Datagram) -> bytes:
    if isinstance(msg, Publish):
        data = _PUBLISH_HDR.pack(_first_byte(T_PUBLISH, msg.qos), msg.session, msg.seq,
                                 msg.msg_id, msg.topic_id) + msg.payload
    elif isinstance(msg, Deliver):
        topic = msg.topic.encode("utf-8")
        if len(topic) > 0xFF:
            raise ValueError("tópico com mais de 255 bytes")
        data = _DELIVER_HDR.pack(_first_byte(T_DELIVER, msg.qos), msg.session, msg.seq,
                                 msg.msg_id, len(topic)) + topic + msg.payload
    elif isinstance(msg, Ack):
        data = _ACK_HDR.pack(T_ACK << 4, msg.session, msg.seq, msg.next)
    else:
        raise TypeError(f"não é um datagrama EventCast: {msg!r}")
    if len(data) > MAX_DATAGRAM:
        raise ValueError(f"datagrama com {len(data)} B excede {MAX_DATAGRAM} B")
    return data


def decode(data: bytes) -> Datagram:
    """Lança DecodeError se estiver vazio, tiver TYPE desconhecido ou o cabeçalho truncado."""
    if not data:
        raise DecodeError("datagrama vazio")
    msg_type = data[0] >> 4
    qos = 2 if data[0] & Q2_BIT else 1

    if msg_type == T_ACK:
        if len(data) != _ACK_HDR.size or data[0] != (T_ACK << 4):
            raise DecodeError(f"ACK inválido: {len(data)} B")
        _, session, seq, nxt = _ACK_HDR.unpack(data)
        return Ack(session, seq, nxt)

    if msg_type == T_PUBLISH:
        if len(data) < _PUBLISH_HDR.size:
            raise DecodeError(f"PUBLISH truncado: {len(data)} B")
        _, session, seq, msg_id, topic_id = _PUBLISH_HDR.unpack_from(data)
        return Publish(session, seq, msg_id, topic_id, qos, bytes(data[_PUBLISH_HDR.size:]))

    if msg_type == T_DELIVER:
        if len(data) < _DELIVER_HDR.size:
            raise DecodeError(f"DELIVER truncado: {len(data)} B")
        _, session, seq, msg_id, top_len = _DELIVER_HDR.unpack_from(data)
        start = _DELIVER_HDR.size + top_len
        if len(data) < start:
            raise DecodeError("DELIVER truncado: TOP_LEN passa o fim do datagrama")
        try:
            topic = bytes(data[_DELIVER_HDR.size:start]).decode("utf-8")
        except UnicodeDecodeError:
            raise DecodeError("tópico com UTF-8 inválido") from None
        return Deliver(session, seq, msg_id, topic, qos, bytes(data[start:]))

    raise DecodeError(f"TYPE desconhecido: 0x{msg_type:x}")