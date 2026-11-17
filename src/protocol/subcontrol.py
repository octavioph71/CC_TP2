"""
SubControl — plano de controlo, sobre TCP (doc/protocolos.md, secção 2). Fase 1.

Mensagem = TYPE u8 · LENGTH u16 · corpo (LENGTH bytes). O TCP é uma stream de bytes:
lê-se o cabeçalho (3 B) e depois exatamente LENGTH bytes (recv_exact).

    HELLO         cliente -> broker   ROLE u8 · UDP_PORT u16 · ID str8
    HELLO_ACK     broker -> cliente   STATUS u8 · DATA_PORT u16 · SESSION u16
    REGISTER      pub -> broker       QOS u8 · TOPIC str8
    REGISTER_ACK  broker -> pub       STATUS u8 · TOPIC_ID u16
    SUBSCRIBE     sub -> broker       QOS u8 · FILTER str8
    SUB_ACK       broker -> sub       STATUS u8
    BYE           qualquer sentido    REASON u8

(str8 = comprimento u8 + bytes UTF-8.) Os tipos 0x07 e 0x08 ficam para o UNSUBSCRIBE (fase 4).
"""

from __future__ import annotations

import socket
import struct
from dataclasses import dataclass
from typing import Union

HEADER = struct.Struct("!BH")
HEADER_SIZE = HEADER.size                      # 3

T_HELLO, T_HELLO_ACK, T_REGISTER, T_REGISTER_ACK, T_SUBSCRIBE, T_SUB_ACK, T_BYE = 1, 2, 3, 4, 5, 6, 9

ROLE_PUBLISHER, ROLE_SUBSCRIBER = 1, 2

ST_OK, ST_BAD_ROLE, ST_ID_IN_USE, ST_MALFORMED, ST_BAD_TOPIC, ST_TOPIC_TAKEN = 0, 1, 2, 3, 4, 5

BYE_NORMAL, BYE_ERROR = 0, 2


class ProtocolError(Exception):
    """Mensagem malformada (tipo desconhecido, corpo com tamanho errado, UTF-8 inválido)."""


class ConnectionClosed(Exception):
    """O outro lado fechou a ligação, possivelmente a meio de uma mensagem."""


@dataclass(frozen=True)
class Hello:
    role: int
    udp_port: int
    node_id: str


@dataclass(frozen=True)
class HelloAck:
    status: int
    data_port: int
    session: int


@dataclass(frozen=True)
class Register:
    qos: int
    topic: str


@dataclass(frozen=True)
class RegisterAck:
    status: int
    topic_id: int


@dataclass(frozen=True)
class Subscribe:
    qos: int
    filter: str


@dataclass(frozen=True)
class SubAck:
    status: int


@dataclass(frozen=True)
class Bye:
    reason: int


Message = Union[Hello, HelloAck, Register, RegisterAck, Subscribe, SubAck, Bye]


def _str8(text: str) -> bytes:
    data = text.encode("utf-8")
    if len(data) > 0xFF:
        raise ValueError(f"cadeia com mais de 255 bytes: {text[:20]}...")
    return bytes([len(data)]) + data


def _body(msg: Message):
    if isinstance(msg, Hello):
        return T_HELLO, struct.pack("!BH", msg.role, msg.udp_port) + _str8(msg.node_id)
    if isinstance(msg, HelloAck):
        return T_HELLO_ACK, struct.pack("!BHH", msg.status, msg.data_port, msg.session)
    if isinstance(msg, Register):
        return T_REGISTER, bytes([msg.qos]) + _str8(msg.topic)
    if isinstance(msg, RegisterAck):
        return T_REGISTER_ACK, struct.pack("!BH", msg.status, msg.topic_id)
    if isinstance(msg, Subscribe):
        return T_SUBSCRIBE, bytes([msg.qos]) + _str8(msg.filter)
    if isinstance(msg, SubAck):
        return T_SUB_ACK, bytes([msg.status])
    if isinstance(msg, Bye):
        return T_BYE, bytes([msg.reason])
    raise TypeError(f"não é uma mensagem SubControl: {msg!r}")


def encode(msg: Message) -> bytes:
    msg_type, body = _body(msg)
    return HEADER.pack(msg_type, len(body)) + body


def _fixed(body: bytes, fmt: str):
    """Corpo só com campos de tamanho fixo: tem de ter exatamente esse tamanho."""
    s = struct.Struct(fmt)
    if len(body) != s.size:
        raise ProtocolError(f"corpo com {len(body)} B (esperava {s.size} B)")
    return s.unpack(body)


def _fixed_then_str8(body: bytes, fmt: str):
    """Campos fixos seguidos de uma str8 que ocupa o resto do corpo."""
    s = struct.Struct(fmt)
    if len(body) < s.size + 1:
        raise ProtocolError(f"corpo curto: {len(body)} B")
    n = body[s.size]
    if len(body) != s.size + 1 + n:
        raise ProtocolError("o comprimento da cadeia não bate com o corpo")
    try:
        text = body[s.size + 1:].decode("utf-8")
    except UnicodeDecodeError:
        raise ProtocolError("cadeia com UTF-8 inválido") from None
    return s.unpack_from(body) + (text,)


def decode(msg_type: int, body: bytes) -> Message:
    """Interpreta o corpo de uma mensagem já delimitada. Só verifica a estrutura."""
    if msg_type == T_HELLO:
        return Hello(*_fixed_then_str8(body, "!BH"))
    if msg_type == T_HELLO_ACK:
        return HelloAck(*_fixed(body, "!BHH"))
    if msg_type == T_REGISTER:
        return Register(*_fixed_then_str8(body, "!B"))
    if msg_type == T_REGISTER_ACK:
        return RegisterAck(*_fixed(body, "!BH"))
    if msg_type == T_SUBSCRIBE:
        return Subscribe(*_fixed_then_str8(body, "!B"))
    if msg_type == T_SUB_ACK:
        return SubAck(*_fixed(body, "!B"))
    if msg_type == T_BYE:
        return Bye(*_fixed(body, "!B"))
    raise ProtocolError(f"tipo de mensagem desconhecido: 0x{msg_type:02x}")


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Lê exatamente n bytes: um recv() pode devolver menos, ou parte da mensagem seguinte."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionClosed(f"EOF ({len(buf)}/{n} B)" if buf else "ligação fechada")
        buf += chunk
    return bytes(buf)


def send_msg(sock: socket.socket, msg: Message) -> None:
    sock.sendall(encode(msg))


def recv_msg(sock: socket.socket) -> Message:
    msg_type, length = HEADER.unpack(recv_exact(sock, HEADER_SIZE))
    return decode(msg_type, recv_exact(sock, length) if length else b"")