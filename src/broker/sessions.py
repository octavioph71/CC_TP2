"""Estado partilhado do broker: sessões, registos e subscrições.

Todas as estruturas são protegidas pelo mesmo Lock. A classe não faz I/O:
quem envia TCP/UDP é sempre broker.py.
"""

import threading

from common.seq import SeqTracker
from common.topics import SubscriptionTable, is_valid_filter, is_valid_topic
from protocol import eventcast as ec
from protocol import subcontrol as sc


class Session:
    def __init__(self, sid, hello, ip, sock):
        self.sid = sid
        self.role = hello.role
        self.node_id = hello.node_id
        self.ip = ip
        self.udp_addr = (ip, hello.udp_port)
        self.sock = sock
        self.topics = {}          # topic_id -> (nome, qos)
        self.tracker = SeqTracker()
        self.next_seq = 0       # sequência para entregas UDP a este subscritor


class SessionStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.sessions = {}      
        self.by_id = {}         
        self.topic_owner = {}  
        self.table = SubscriptionTable()
        self.next_sid = 1

    def create(self, hello, ip, sock):
        """Devolve a sessão ou None se o node_id já estiver em uso."""
        with self.lock:
            if hello.node_id in self.by_id:
                return None
            # SESSION é u16 no cabeçalho: nunca reutilizar uma sessão ativa.
            for _ in range(65535):
                sid = self.next_sid
                self.next_sid = sid % 65535 + 1
                if sid not in self.sessions:
                    session = Session(sid, hello, ip, sock)
                    self.sessions[sid] = session
                    self.by_id[hello.node_id] = sid
                    return session
            return None

    def remove(self, session):
        if session is None:
            return
        with self.lock:
            if self.sessions.pop(session.sid, None) is not session:
                return
            self.by_id.pop(session.node_id, None)
            self.table.remove_session(session.sid)
            for topic, _ in session.topics.values():
                self.topic_owner.pop(topic, None)

    def register(self, session, msg):
        if msg.qos not in (1, 2):
            return sc.RegisterAck(sc.ST_MALFORMED, 0)
        if not is_valid_topic(msg.topic):
            return sc.RegisterAck(sc.ST_BAD_TOPIC, 0)
        with self.lock:
            if msg.topic in self.topic_owner and self.topic_owner[msg.topic] != session.sid:
                return sc.RegisterAck(sc.ST_TOPIC_TAKEN, 0)
            for tid, (name, _) in session.topics.items():
                if name == msg.topic:
                    session.topics[tid] = (name, msg.qos)
                    return sc.RegisterAck(sc.ST_OK, tid)
            tid = len(session.topics) + 1
            if tid > 65535:
                return sc.RegisterAck(sc.ST_MALFORMED, 0)
            session.topics[tid] = (msg.topic, msg.qos)
            self.topic_owner[msg.topic] = session.sid
            return sc.RegisterAck(sc.ST_OK, tid)

    def subscribe(self, session, msg):
        if msg.qos not in (1, 2):
            return sc.SubAck(sc.ST_MALFORMED)
        if not is_valid_filter(msg.filter):
            return sc.SubAck(sc.ST_BAD_TOPIC)
        with self.lock:
            self.table.add(session.sid, msg.filter, msg.qos)
        return sc.SubAck(sc.ST_OK)

    def publisher(self, msg, ip):
        """Requer lock adquirido pelo chamador."""
        s = self.sessions.get(msg.session)
        if s and s.role == sc.ROLE_PUBLISHER and s.ip == ip and msg.topic_id in s.topics:
            return s
        return None

    def destinations(self, msg, topic, flow_qos):
        """Prepara cada entrega, UMA por subscritor, sem fazer sendto.

        Requer lock adquirido pelo chamador.
        """
        result = []
        for sid, sub_qos in self.table.match(topic).items():
            sub = self.sessions.get(sid)
            if sub is None:
                continue
            deliver = ec.Deliver(sid, sub.next_seq, msg.msg_id, topic,
                                 min(sub_qos, flow_qos), msg.payload)
            sub.next_seq = (sub.next_seq + 1) % 65536
            result.append((sub.udp_addr, ec.encode(deliver)))
        return result