import socket
import threading
import time

from protocol import subcontrol as sc

"""Abertura de sessão SubControl, comum ao publicador e ao subscritor."""

class SessionError(Exception):
    """Não foi possível estabelecer a sessão (broker em baixo, recusa, resposta inválida...)."""

def open_session(endpoint, role: int, node_id: str, udp_port: int, stop: threading.Event,
                 connect_timeout: float = 10.0, hello_timeout: float = 5.0):
    """
    TCP + HELLO/HELLO_ACK. Devolve (socket, HelloAck).

    Os processos arrancam ao mesmo tempo, por isso o broker pode ainda não estar à escuta: a
    ligação repete-se a cada 200 ms até `connect_timeout`. Se o broker aceitar o TCP mas não
    responder ao HELLO, desiste ao fim de `hello_timeout`.
    """
    deadline = time.monotonic() + connect_timeout
    while True:
        try:
            sock = socket.create_connection(endpoint.as_tuple(), timeout=2.0)
            break
        except OSError as e:
            if stop.is_set() or time.monotonic() >= deadline:
                raise SessionError(f"não consegui ligar a {endpoint}: {e}") from None
            stop.wait(0.2)

    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.settimeout(hello_timeout)
    try:
        sc.send_msg(sock, sc.Hello(role, udp_port, node_id))
        ack = sc.recv_msg(sock)
    except (OSError, sc.ConnectionClosed, sc.ProtocolError) as e:
        sock.close()
        raise SessionError(f"sem resposta ao HELLO: {e}") from None

    if not isinstance(ack, sc.HelloAck) or ack.status != sc.ST_OK:
        sock.close()
        raise SessionError(f"sessão recusada: {ack}")
    sock.settimeout(None)
    return sock, ack