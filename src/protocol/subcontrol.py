"""
SubControl — plano de controlo, sobre TCP (docs/protocolos.md, secção 2).

Cada mensagem tem um cabeçalho fixo de 3 bytes, seguido do corpo:

    +-------+-------+-------+
    | TYPE  |    LENGTH     |   TYPE u8 · LENGTH u16 (bytes do corpo)
    +-------+-------+-------+
    |  corpo (LENGTH bytes) ...

Este módulo só trata de bytes: construir, interpretar, enviar e receber
mensagens. Não sabe nada de papéis — as regras (papel inválido, tópico já
registado, ...) são do broker.

Uso típico:
    ch = Channel(sock)
    ch.send(Hello(ROLE_PUBLISHER, udp_port, "pub1"))
    msg = ch.recv()                     # -> HelloAck(...)
"""

from __future__ import annotations

import socket
import struct
import threading
from dataclasses import dataclass
from typing import Union

# --------------------------------------------------------------------------- #
#  Constantes
# --------------------------------------------------------------------------- #

HEADER = struct.Struct("!BH")          # TYPE u8 · LENGTH u16
HEADER_SIZE = HEADER.size              # 3
MAX_BODY = 0xFFFF                      # LENGTH é u16

# Tipos de mensagem
T_HELLO        = 0x01
T_HELLO_ACK    = 0x02
T_REGISTER     = 0x03
T_REGISTER_ACK = 0x04
T_SUBSCRIBE    = 0x05
T_SUB_ACK      = 0x06
T_UNSUBSCRIBE  = 0x07
T_UNSUB_ACK    = 0x08
T_BYE          = 0x09
# 0x10–0x1F: reservado para broker ↔ broker [F4]

# ROLE
ROLE_PUBLISHER  = 1
ROLE_SUBSCRIBER = 2
ROLE_BROKER     = 3

# STATUS
ST_OK               = 0
ST_BAD_ROLE         = 1
ST_ID_IN_USE        = 2
ST_MALFORMED        = 3
ST_BAD_TOPIC        = 4
ST_TOPIC_TAKEN      = 5
ST_NOT_SUBSCRIBED   = 6

# REASON (BYE)
BYE_NORMAL  = 0
BYE_EVICTED = 1
BYE_ERROR   = 2


class ProtocolError(Exception):
    """Mensagem malformada: tipo desconhecido, corpo curto, cadeia inválida..."""


class ConnectionClosed(Exception):
    """O outro lado fechou a ligação (EOF), possivelmente a meio de uma mensagem."""


# --------------------------------------------------------------------------- #
#  Mensagens
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Hello:            # cliente -> broker
    role: int
    udp_port: int
    node_id: str


@dataclass(frozen=True)
class HelloAck:         # broker -> cliente
    status: int
    data_port: int
    session: int


@dataclass(frozen=True)
class Register:         # publicador -> broker
    qos: int
    topic: str


@dataclass(frozen=True)
class RegisterAck:      # broker -> publicador
    status: int
    topic_id: int


@dataclass(frozen=True)
class Subscribe:        # subscritor -> broker
    req_id: int
    qos: int
    filter: str


@dataclass(frozen=True)
class SubAck:           # broker -> subscritor
    req_id: int
    status: int


@dataclass(frozen=True)
class Unsubscribe:      # subscritor -> broker
    req_id: int
    filter: str


@dataclass(frozen=True)
class UnsubAck:         # broker -> subscritor
    req_id: int
    status: int


@dataclass(frozen=True)
class Bye:              # qualquer sentido
    reason: int


Message = Union[Hello, HelloAck, Register, RegisterAck, Subscribe,
                SubAck, Unsubscribe, UnsubAck, Bye]


# --------------------------------------------------------------------------- #
#  Escrita e leitura de campos
# --------------------------------------------------------------------------- #

def _str8(text: str) -> bytes:
    """STR8: u8 comprimento + bytes UTF-8."""
    data = text.encode("utf-8")
    if len(data) > 0xFF:
        raise ValueError(f"cadeia com mais de 255 bytes: {text[:20]}...")
    return bytes([len(data)]) + data


class _Reader:
    """Lê campos de um corpo, verificando sempre se há bytes suficientes."""

    def __init__(self, body: bytes) -> None:
        self._body = body
        self._pos = 0

    def _take(self, n: int) -> bytes:
        end = self._pos + n
        if end > len(self._body):
            raise ProtocolError(
                f"corpo curto: precisava de {n} B na posição {self._pos}, "
                f"o corpo tem {len(self._body)} B")
        chunk = self._body[self._pos:end]
        self._pos = end
        return chunk

    def u8(self) -> int:
        return self._take(1)[0]

    def u16(self) -> int:
        return struct.unpack("!H", self._take(2))[0]

    def str8(self) -> str:
        n = self.u8()
        try:
            return self._take(n).decode("utf-8")
        except UnicodeDecodeError:
            raise ProtocolError("cadeia com UTF-8 inválido") from None

    def end(self) -> None:
        """Garante que não sobram bytes no corpo."""
        if self._pos != len(self._body):
            raise ProtocolError(
                f"sobram {len(self._body) - self._pos} B no fim do corpo")


# --------------------------------------------------------------------------- #
#  Construir (encode) e interpretar (decode)
# --------------------------------------------------------------------------- #

def _body(msg: Message):
    """Devolve (TYPE, corpo em bytes) de uma mensagem."""
    if isinstance(msg, Hello):
        return T_HELLO, struct.pack("!BH", msg.role, msg.udp_port) + _str8(msg.node_id)
    if isinstance(msg, HelloAck):
        return T_HELLO_ACK, struct.pack("!BHH", msg.status, msg.data_port, msg.session)
    if isinstance(msg, Register):
        return T_REGISTER, struct.pack("!B", msg.qos) + _str8(msg.topic)
    if isinstance(msg, RegisterAck):
        return T_REGISTER_ACK, struct.pack("!BH", msg.status, msg.topic_id)
    if isinstance(msg, Subscribe):
        return T_SUBSCRIBE, struct.pack("!HB", msg.req_id, msg.qos) + _str8(msg.filter)
    if isinstance(msg, SubAck):
        return T_SUB_ACK, struct.pack("!HB", msg.req_id, msg.status)
    if isinstance(msg, Unsubscribe):
        return T_UNSUBSCRIBE, struct.pack("!H", msg.req_id) + _str8(msg.filter)
    if isinstance(msg, UnsubAck):
        return T_UNSUB_ACK, struct.pack("!HB", msg.req_id, msg.status)
    if isinstance(msg, Bye):
        return T_BYE, struct.pack("!B", msg.reason)
    raise TypeError(f"não é uma mensagem SubControl: {msg!r}")


def encode(msg: Message) -> bytes:
    """Mensagem completa: cabeçalho de 3 B + corpo."""
    msg_type, body = _body(msg)
    if len(body) > MAX_BODY:
        raise ValueError(f"corpo demasiado grande: {len(body)} B")
    return HEADER.pack(msg_type, len(body)) + body


def decode(msg_type: int, body: bytes) -> Message:
    """
    Interpreta o corpo de uma mensagem já delimitada.
    Só verifica a estrutura (tamanhos, UTF-8); os valores são validados por quem a recebe.
    """
    r = _Reader(body)

    if msg_type == T_HELLO:
        msg = Hello(role=r.u8(), udp_port=r.u16(), node_id=r.str8())
    elif msg_type == T_HELLO_ACK:
        msg = HelloAck(status=r.u8(), data_port=r.u16(), session=r.u16())
    elif msg_type == T_REGISTER:
        msg = Register(qos=r.u8(), topic=r.str8())
    elif msg_type == T_REGISTER_ACK:
        msg = RegisterAck(status=r.u8(), topic_id=r.u16())
    elif msg_type == T_SUBSCRIBE:
        msg = Subscribe(req_id=r.u16(), qos=r.u8(), filter=r.str8())
    elif msg_type == T_SUB_ACK:
        msg = SubAck(req_id=r.u16(), status=r.u8())
    elif msg_type == T_UNSUBSCRIBE:
        msg = Unsubscribe(req_id=r.u16(), filter=r.str8())
    elif msg_type == T_UNSUB_ACK:
        msg = UnsubAck(req_id=r.u16(), status=r.u8())
    elif msg_type == T_BYE:
        msg = Bye(reason=r.u8())
    else:
        raise ProtocolError(f"tipo de mensagem desconhecido: 0x{msg_type:02x}")

    r.end()
    return msg


# --------------------------------------------------------------------------- #
#  Envio e receção sobre a stream TCP
# --------------------------------------------------------------------------- #

def recv_exact(sock: socket.socket, n: int) -> bytes:
    """
    Lê exatamente n bytes. Um recv() pode devolver menos do que o pedido
    (ou parte da mensagem seguinte), por isso repete-se até completar.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            if buf:
                raise ConnectionClosed(f"EOF a meio de uma mensagem ({len(buf)}/{n} B)")
            raise ConnectionClosed("ligação fechada pelo outro lado")
        buf += chunk
    return bytes(buf)


def send_msg(sock: socket.socket, msg: Message) -> None:
    sock.sendall(encode(msg))


def recv_msg(sock: socket.socket) -> Message:
    """Lê o cabeçalho (3 B), depois o corpo (LENGTH B), e interpreta-o."""
    msg_type, length = HEADER.unpack(recv_exact(sock, HEADER_SIZE))
    body = recv_exact(sock, length) if length else b""
    return decode(msg_type, body)


class Channel:
    """
    Uma ligação SubControl. O envio é protegido por um lock, porque no broker
    mais do que uma thread pode escrever no mesmo socket (ex.: a thread da
    sessão e a que expulsa subscritores). A receção é feita por uma só thread.
    """

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self._send_lock = threading.Lock()

    def send(self, msg: Message) -> None:
        data = encode(msg)
        with self._send_lock:
            self.sock.sendall(data)

    def recv(self) -> Message:
        return recv_msg(self.sock)

    def peer(self):
        """(ip, porta) do outro lado."""
        return self.sock.getpeername()

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()