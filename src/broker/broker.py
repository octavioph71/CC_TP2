"""
Broker, fase 1 (doc/protocolos.md). Três tipos de thread:

    aceitar   TCP: aceita ligações
    sessão    TCP: uma por cliente (HELLO, REGISTER, SUBSCRIBE, BYE)
    dados     UDP: recebe PUBLISH e entrega a quem subscreveu

Quem fala na porta UDP é identificado pelo SESSION do datagrama (atribuído no HELLO_ACK),
confirmado com o IP de origem. Um datagrama de uma sessão desconhecida é descartado.
"""

import socket
import sys
import threading

from common.seq import NEW, SeqTracker
from common.topics import SubscriptionTable, is_valid_filter, is_valid_topic
from protocol import eventcast as ec
from protocol import subcontrol as sc

HELLO_TIMEOUT = 5.0


class Violation(Exception):
    """O cliente fez algo que o protocolo não permite: BYE(2) e fecha-se a sessão."""


class Session:
    """O que o broker sabe de um cliente."""

    def __init__(self, sid, role, node_id, ip, udp_port, sock):
        self.sid, self.role, self.node_id = sid, role, node_id
        self.ip, self.udp_addr, self.sock = ip, (ip, udp_port), sock
        self.topics = {}              
        self.tracker = SeqTracker()   
        self.next_seq = 0             


class Broker:
    def __init__(self, cfg, node_id, stop):
        self.cfg, self.node_id, self.stop = cfg, node_id, stop
        self.lock = threading.Lock()          
        self.sessions = {}                    
        self.by_id = {}
        self.topic_owner = {}                 
        self.table = SubscriptionTable()     
        self.next_sid = 1

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
        with self.lock:
            sessions = list(self.sessions.values())
        for s in sessions:
            self._send_bye(s.sock, sc.BYE_NORMAL)
            s.sock.close()
        self.tcp.close()
        self.udp.close()

    def _accept_loop(self):
        while not self.stop.is_set():
            try:
                conn, addr = self.tcp.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn, addr[0]), daemon=True).start()

    def _serve(self, conn, ip):
        """Uma thread por cliente: handshake e depois pedidos até ao BYE."""
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        session = None
        try:
            session = self._handshake(conn, ip)
            while session:
                msg = sc.recv_msg(conn)
                if isinstance(msg, sc.Bye):
                    break
                self._handle(session, msg)
        except (sc.ConnectionClosed, OSError):
            pass                                        
        except (sc.ProtocolError, Violation) as e:
            self.log(f"violação do protocolo ({e}): a fechar a sessão")
            self._send_bye(conn, sc.BYE_ERROR)
        finally:
            self._forget(session)
            conn.close()

    def _handshake(self, conn, ip):
        """HELLO -> HELLO_ACK (traz a porta de dados). Devolve a sessão, ou None se foi recusada."""
        conn.settimeout(HELLO_TIMEOUT)                  
        hello = sc.recv_msg(conn)
        if not isinstance(hello, sc.Hello):
            raise Violation("pedido antes do HELLO")
        status, session = self._check_hello(hello), None
        if status == sc.ST_OK:
            session = self._add_session(hello, conn, ip)
            status = sc.ST_OK if session else sc.ST_ID_IN_USE
        sid = session.sid if session else 0
        sc.send_msg(conn, sc.HelloAck(status, self.cfg.data_port if session else 0, sid))
        conn.settimeout(None)
        if session:
            self.log(f"sessão {sid}: {hello.node_id} ({ip}:{hello.udp_port})")
        return session

    def _check_hello(self, hello):
        if hello.role not in (sc.ROLE_PUBLISHER, sc.ROLE_SUBSCRIBER):
            return sc.ST_BAD_ROLE
        if hello.udp_port == 0 or not hello.node_id:
            return sc.ST_MALFORMED
        return sc.ST_OK

    def _add_session(self, hello, conn, ip):
        """Cria a sessão com o próximo SESSION, ou devolve None se o id já está em uso."""
        with self.lock:
            if hello.node_id in self.by_id:
                return None
            s = Session(self.next_sid, hello.role, hello.node_id, ip, hello.udp_port, conn)
            self.next_sid += 1
            self.sessions[s.sid] = s
            self.by_id[s.node_id] = s.sid
            return s

    def _handle(self, s, msg):
        if isinstance(msg, sc.Register) and s.role == sc.ROLE_PUBLISHER:
            sc.send_msg(s.sock, self._register(s, msg))
        elif isinstance(msg, sc.Subscribe) and s.role == sc.ROLE_SUBSCRIBER:
            sc.send_msg(s.sock, self._subscribe(s, msg))
        else:
            raise Violation(f"{type(msg).__name__} não é permitido a este papel")

    def _register(self, s, msg):
        if msg.qos not in (1, 2):
            return sc.RegisterAck(sc.ST_MALFORMED, 0)
        if not is_valid_topic(msg.topic):
            return sc.RegisterAck(sc.ST_BAD_TOPIC, 0)
        with self.lock:
            if self.topic_owner.setdefault(msg.topic, s.sid) != s.sid:
                return sc.RegisterAck(sc.ST_TOPIC_TAKEN, 0)  
            tid = len(s.topics) + 1
            s.topics[tid] = (msg.topic, msg.qos)
        return sc.RegisterAck(sc.ST_OK, tid)

    def _subscribe(self, s, msg):
        if msg.qos not in (1, 2):
            return sc.SubAck(sc.ST_MALFORMED)
        if not is_valid_filter(msg.filter):
            return sc.SubAck(sc.ST_BAD_TOPIC)
        with self.lock:
            self.table.add(s.sid, msg.filter, msg.qos)
        return sc.SubAck(sc.ST_OK)

    def _forget(self, s):
        """Esquece a sessão: filtros e tópicos. Seguro de chamar com None."""
        if s is None:
            return
        with self.lock:
            del self.sessions[s.sid]
            del self.by_id[s.node_id]
            self.table.remove_session(s.sid)
            for topic, _ in s.topics.values():
                self.topic_owner.pop(topic, None)

    def _send_bye(self, sock, reason):
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
                return
            except ec.DecodeError:
                continue                              
            if isinstance(msg, ec.Publish):
                self._on_publish(msg, addr[0])

    def _on_publish(self, msg, ip):
        with self.lock:
            pub = self._publisher_of(msg, ip)
            if pub and self._in_sequence(pub, msg.seq):
                topic, flow_qos = pub.topics[msg.topic_id]
                self._fan_out(msg, topic, flow_qos)

    def _publisher_of(self, msg, ip):
        """Quem é este? A sessão do datagrama, se for um publicador, vier do seu IP e o tópico for seu."""
        s = self.sessions.get(msg.session)
        if s and s.role == sc.ROLE_PUBLISHER and s.ip == ip and msg.topic_id in s.topics:
            return s
        return None

    def _in_sequence(self, pub, seq):
        """Confere o SEQ com o esperado. False se for repetido ou estiver fora da janela."""
        expected = pub.tracker.next
        if pub.tracker.receive(seq) != NEW:
            self.log(f"{pub.node_id}: SEQ {seq} repetido ou fora da janela: descartado")
            return False
        if seq != expected:
            self.log(f"{pub.node_id}: esperava SEQ {expected}, chegou {seq} (perda ou fora de ordem)")
        return True

    def _fan_out(self, msg, topic, flow_qos):
        """Só quem subscreveu, uma cópia por subscritor, com o mais fraco de (filtro, fluxo)."""
        for sid, sub_qos in self.table.match(topic).items():
            sub = self.sessions[sid]                  
            deliver = ec.Deliver(sid, sub.next_seq, msg.msg_id, topic, min(sub_qos, flow_qos), msg.payload)
            sub.next_seq = (sub.next_seq + 1) % 0x10000
            try:
                self.udp.sendto(ec.encode(deliver), sub.udp_addr)
            except OSError:
                pass                                   

def run(node_id, cfg, stop):
    broker = Broker(cfg, node_id, stop)
    try:
        broker.start()
    except OSError as e:
        print(f"[{node_id}] não consegui abrir as portas: {e}", file=sys.stderr)
        return 1
    by_signal = stop.wait(cfg.duration)
    stop.set()
    broker.shutdown()
    broker.log(f"fim ({'sinal' if by_signal else 'duration'})")
    return 0