"""Broker: TCP para controlo; UDP para receber e encaminhar publicações.

A gestão do estado (sessões, tópicos, filtros) está em sessions.py.
Esta é a versão funcional da Fase 1; ACK/retransmissões ficam para a Fase 2.
"""

import socket
import sys
import threading

from broker.sessions import SessionStore
from protocol import eventcast as ec
from protocol import subcontrol as sc

HELLO_TIMEOUT = 5.0


class Violation(Exception):
    """Mensagem não permitida para a sessão."""


class Broker:
    def __init__(self, cfg, node_id, stop):
        self.cfg, self.node_id, self.stop = cfg, node_id, stop
        self.state = SessionStore()

    def log(self, text):
        print(f"[{self.node_id}] {text}", flush=True)

    def start(self):
        self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp.bind((self.cfg.bind, self.cfg.control_port))
        self.tcp.listen(64)
        self.tcp.settimeout(0.2)
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind((self.cfg.bind, self.cfg.data_port))
        self.udp.settimeout(0.1)
        threading.Thread(target=self._accept_loop, daemon=True).start()
        threading.Thread(target=self._data_loop, daemon=True).start()
        self.log(f"à escuta em {self.cfg.bind} (TCP {self.cfg.control_port}, UDP {self.cfg.data_port})")

    def shutdown(self):
        with self.state.lock:
            sessions = list(self.state.sessions.values())
        for session in sessions:
            self._send_bye(session.sock, sc.BYE_NORMAL)
            try:
                session.sock.close()
            except OSError:
                pass
        self.tcp.close()
        self.udp.close()

    def _accept_loop(self):
        while not self.stop.is_set():
            try:
                conn, addr = self.tcp.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._serve, args=(conn, addr[0]), daemon=True).start()

    def _serve(self, conn, ip):
        """Uma thread por ligação TCP."""
        session = None
        try:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            session = self._handshake(conn, ip)
            while session and not self.stop.is_set():
                msg = sc.recv_msg(conn)
                if isinstance(msg, sc.Bye):
                    break
                self._handle(session, msg)
        except (sc.ConnectionClosed, OSError):
            pass
        except (sc.ProtocolError, Violation) as exc:
            self.log(f"violação do protocolo ({exc}): a fechar a sessão")
            self._send_bye(conn, sc.BYE_ERROR)
        finally:
            self.state.remove(session)
            conn.close()

    def _handshake(self, conn, ip):
        """HELLO -> HELLO_ACK (traz a porta de dados). Devolve a sessão, ou None se o id já está em uso."""
        conn.settimeout(HELLO_TIMEOUT)
        hello = sc.recv_msg(conn)
        if not isinstance(hello, sc.Hello):
            raise Violation("pedido antes do HELLO")
        if hello.role not in (sc.ROLE_PUBLISHER, sc.ROLE_SUBSCRIBER) or not hello.node_id or hello.udp_port == 0:
            raise Violation("HELLO inválido")
        session = self.state.create(hello, ip, conn)
        if session is None:
            sc.send_msg(conn, sc.HelloAck(sc.ST_ID_IN_USE, 0, 0))
            return None
        sc.send_msg(conn, sc.HelloAck(sc.ST_OK, self.cfg.data_port, session.sid))
        conn.settimeout(None)
        self.log(f"sessão {session.sid}: {hello.node_id} ({ip}:{hello.udp_port})")
        return session

    def _handle(self, session, msg):
        if isinstance(msg, sc.Register) and session.role == sc.ROLE_PUBLISHER:
            reply = self.state.register(session, msg)
        elif isinstance(msg, sc.Subscribe) and session.role == sc.ROLE_SUBSCRIBER:
            reply = self.state.subscribe(session, msg)
        else:
            raise Violation(f"{type(msg).__name__} não é permitido a este papel")
        sc.send_msg(session.sock, reply)

    @staticmethod
    def _send_bye(sock, reason):
        try:
            sc.send_msg(sock, sc.Bye(reason))
        except OSError:
            pass

    def _data_loop(self):
        while not self.stop.is_set():
            try:
                data, addr = self.udp.recvfrom(ec.RECV_BUFSIZE)
                msg = ec.decode(data)
            except socket.timeout:
                continue
            except OSError:
                break
            except ec.DecodeError:
                continue
            if isinstance(msg, ec.Publish):
                self._on_publish(msg, addr[0])

    def _on_publish(self, msg, ip):
        with self.state.lock:
            pub = self.state.publisher(msg, ip)
            if pub is None:
                return
            expected = pub.tracker.next
            if not pub.tracker.receive(msg.seq):
                self.log(f"{pub.node_id}: SEQ {msg.seq} repetido/fora da janela: descartado")
                return
            if msg.seq != expected:
                self.log(f"{pub.node_id}: esperava SEQ {expected}, chegou {msg.seq}")
            topic, flow_qos = pub.topics[msg.topic_id]
            destinations = self.state.destinations(msg, topic, flow_qos)

        for address, packet in destinations:
            try:
                self.udp.sendto(packet, address)
            except OSError:
                pass

def run(node_id, cfg, stop):
    broker = Broker(cfg, node_id, stop)
    try:
        broker.start()
    except OSError as exc:
        print(f"[{node_id}] não consegui abrir as portas: {exc}", file=sys.stderr)
        return 1
    by_signal = stop.wait(cfg.duration)
    stop.set()
    broker.shutdown()
    broker.log(f"fim ({'sinal' if by_signal else 'duration'})")
    return 0