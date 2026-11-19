"""
SubControl — plano de controlo, sobre TCP (doc/protocolos.md, secção 1).

Mensagem = TYPE (1 B) · LENGTH (2 B) · corpo (LENGTH B). O TCP é uma stream de bytes:
lê-se o cabeçalho (3 B) e depois exatamente LENGTH bytes (recv_exact).

    HELLO         cliente -> broker   ROLE 1 B · UDP_PORT 2 B · ID str8
    HELLO_ACK     broker -> cliente   STATUS 1 B · DATA_PORT 2 B · SESSION 2 B
    REGISTER      pub -> broker       QOS 1 B · TOPIC str8
    REGISTER_ACK  broker -> pub       STATUS 1 B · TOPIC_ID 2 B
    SUBSCRIBE     sub -> broker       QOS 1 B · FILTER str8
    SUB_ACK       broker -> sub       STATUS 1 B
    BYE           qualquer sentido    REASON 1 B

str8 = 1 byte com o comprimento + o texto em UTF-8. Os números de 2 B são big-endian.
Os tipos 0x07 e 0x08 ficam para o UNSUBSCRIBE (fase 4).
"""

from dataclasses import dataclass
from typing import Union

HEADER_SIZE = 3

T_HELLO, T_HELLO_ACK, T_REGISTER, T_REGISTER_ACK, T_SUBSCRIBE, T_SUB_ACK, T_BYE = 1, 2, 3, 4, 5, 6, 9

ROLE_PUBLISHER, ROLE_SUBSCRIBER = 1, 2

ST_OK, ST_ID_IN_USE, ST_MALFORMED, ST_BAD_TOPIC, ST_TOPIC_TAKEN = 0, 2, 3, 4, 5

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

def _u16(n):
    """Número -> 2 bytes (big-endian)."""
    return n.to_bytes(2, "big")


def _u16_at(data, pos):
    """2 bytes a partir de `pos` -> número."""
    return int.from_bytes(data[pos:pos + 2], "big")


def _str8(text):
    """Texto -> 1 byte de comprimento + UTF-8."""
    data = text.encode("utf-8")
    if len(data) > 255:
        raise ValueError(f"cadeia com mais de 255 bytes: {text[:20]}...")
    return bytes([len(data)]) + data


def _str8_at(body, pos):
    """Lê a str8 que começa em `pos` e ocupa o resto do corpo."""
    if len(body) <= pos:
        raise ProtocolError("corpo curto")
    if len(body) != pos + 1 + body[pos]:
        raise ProtocolError("o comprimento da cadeia não bate com o corpo")
    try:
        return body[pos + 1:].decode("utf-8")
    except UnicodeDecodeError:
        raise ProtocolError("cadeia com UTF-8 inválido") from None

def _expect_len(body, n):
    if len(body) != n:
        raise ProtocolError(f"corpo com {len(body)} B (esperava {n} B)")

def encode(msg):
    if isinstance(msg, Hello):
        msg_type = T_HELLO
        body = bytes([msg.role]) + _u16(msg.udp_port) + _str8(msg.node_id)
    elif isinstance(msg, HelloAck):
        msg_type = T_HELLO_ACK
        body = bytes([msg.status]) + _u16(msg.data_port) + _u16(msg.session)
    elif isinstance(msg, Register):
        msg_type = T_REGISTER
        body = bytes([msg.qos]) + _str8(msg.topic)
    elif isinstance(msg, RegisterAck):
        msg_type = T_REGISTER_ACK
        body = bytes([msg.status]) + _u16(msg.topic_id)
    elif isinstance(msg, Subscribe):
        msg_type = T_SUBSCRIBE
        body = bytes([msg.qos]) + _str8(msg.filter)
    elif isinstance(msg, SubAck):
        msg_type = T_SUB_ACK
        body = bytes([msg.status])
    elif isinstance(msg, Bye):
        msg_type = T_BYE
        body = bytes([msg.reason])
    else:
        raise TypeError(f"não é uma mensagem SubControl: {msg!r}")
    return bytes([msg_type]) + _u16(len(body)) + body

def decode(msg_type, body):
    """Interpreta o corpo de uma mensagem já delimitada. Só verifica a estrutura."""
    if msg_type == T_HELLO:
        node_id = _str8_at(body, 3)                    
        return Hello(role=body[0], udp_port=_u16_at(body, 1), node_id=node_id)
    if msg_type == T_HELLO_ACK:
        _expect_len(body, 5)
        return HelloAck(status=body[0], data_port=_u16_at(body, 1), session=_u16_at(body, 3))
    if msg_type == T_REGISTER:
        topic = _str8_at(body, 1)                       
        return Register(qos=body[0], topic=topic)
    if msg_type == T_REGISTER_ACK:
        _expect_len(body, 3)
        return RegisterAck(status=body[0], topic_id=_u16_at(body, 1))
    if msg_type == T_SUBSCRIBE:
        filtro = _str8_at(body, 1)                      
        return Subscribe(qos=body[0], filter=filtro)
    if msg_type == T_SUB_ACK:
        _expect_len(body, 1)
        return SubAck(status=body[0])
    if msg_type == T_BYE:
        _expect_len(body, 1)
        return Bye(reason=body[0])
    raise ProtocolError(f"tipo de mensagem desconhecido: 0x{msg_type:02x}")


def recv_exact(sock, n):
    """Lê exatamente n bytes: um recv() pode devolver menos, ou parte da mensagem seguinte."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionClosed(f"EOF ({len(buf)}/{n} B)" if buf else "ligação fechada")
        buf += chunk
    return bytes(buf)


def send_msg(sock, msg):
    sock.sendall(encode(msg))


def recv_msg(sock):
    header = recv_exact(sock, HEADER_SIZE)
    length = _u16_at(header, 1)
    body = recv_exact(sock, length) if length else b""
    return decode(header[0], body)