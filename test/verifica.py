import os
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from common.seq import SeqTracker
from common.topics import SubscriptionTable
from protocol import eventcast as ec
from protocol import subcontrol as sc

"""Testes unitários da fase 1:  python3 test/verifica.py"""

falhas = 0

def check(nome, ok):
    global falhas
    print(("  OK    " if ok else "  FALHA ") + nome)
    falhas += not ok

def lanca(exc, fn, *a):
    try:
        fn(*a)
    except exc:
        return True
    return False

print("[SubControl]")
msgs = [sc.Hello(1, 40001, "pub1"), sc.HelloAck(0, 7311, 5), sc.Register(2, "north/station7/temperature"),
        sc.RegisterAck(0, 3), sc.Subscribe(1, "north/*"), sc.SubAck(0), sc.Bye(0)]
check("ida e volta de todas as mensagens", all(sc.decode(sc.encode(m)[0], sc.encode(m)[3:]) == m for m in msgs))
check("HELLO de pub1 = 11 B", len(sc.encode(msgs[0])) == 11)
a, b = socket.socketpair()
a.sendall(b"".join(sc.encode(m) for m in msgs[:3]))                     
check("mensagens coladas lidas uma a uma", [sc.recv_msg(b) for _ in range(3)] == msgs[:3])
a.sendall(sc.encode(msgs[0])[:5]); a.close()
check("EOF a meio de uma mensagem", lanca(sc.ConnectionClosed, sc.recv_msg, b))
check("tipo desconhecido", lanca(sc.ProtocolError, sc.decode, 0x7F, b""))
check("corpo curto", lanca(sc.ProtocolError, sc.decode, sc.T_HELLO_ACK, b"\x00\x1c"))
check("str8 maior do que o corpo", lanca(sc.ProtocolError, sc.decode, sc.T_REGISTER, b"\x01\x09ab"))

print("[EventCast]")
p = ec.Publish(1, 0, 1, 1, 1, b"23.5")
d = ec.Deliver(2, 0, 1, "north/station7/temperature", 2, b"23.5")
check("ida e volta", ec.decode(ec.encode(p)) == p and ec.decode(ec.encode(d)) == d)
check("PUBLISH = 11 B + conteúdo", len(ec.encode(p)) == 15)
a = ec.Ack(2, 42, 43)
check("ACK recuperado: 7 B e ida e volta", len(ec.encode(a)) == 7 and ec.decode(ec.encode(a)) == a)
check("ACK truncado ou longo rejeitado", all(lanca(ec.DecodeError, ec.decode, ec.encode(a)[:n]) for n in range(7))
      and lanca(ec.DecodeError, ec.decode, ec.encode(a) + b"X"))
check("primeiro byte: TYPE e Q2", ec.encode(p)[0] == 0x10 and ec.encode(d)[0] == 0x28)
check("todos os truncamentos do cabeçalho rejeitados",
      all(lanca(ec.DecodeError, ec.decode, ec.encode(p)[:n]) for n in range(11))
      and all(lanca(ec.DecodeError, ec.decode, ec.encode(d)[:n]) for n in range(10 + 26)))
check("datagrama vazio e TYPE desconhecido", lanca(ec.DecodeError, ec.decode, b"")
      and lanca(ec.DecodeError, ec.decode, b"\xf0" + bytes(10)))

print("[Estado do broker]")
from broker.sessions import SessionStore
store = SessionStore()
hello = sc.Hello(sc.ROLE_PUBLISHER, 42001, "pub1")
obj = object()
sess = store.create(hello, "127.0.0.1", obj)
check("cria sessão, não duplica ID", sess is not None and store.create(hello, "127.0.0.1", obj) is None)
check("regista tópico e reutiliza o mesmo ID", store.register(sess, sc.Register(1, "north/station7/temperature")).topic_id == 1
      and store.register(sess, sc.Register(1, "north/station7/temperature")).topic_id == 1)
check("identifica publicador UDP pela sessão/IP/tópico", store.publisher(ec.Publish(sess.sid, 0, 1, 1, 1, b""), "127.0.0.1") is sess
      and store.publisher(ec.Publish(sess.sid, 0, 1, 1, 1, b""), "127.0.0.2") is None)
store.remove(sess)
check("remove sessão e liberta identificação", store.create(hello, "127.0.0.1", obj) is not None)

print("[SEQ esperado]")
t = SeqTracker()
check("0 e 1 por ordem", t.receive(0) and t.receive(1) and t.next == 2)
check("3 antes do 2: buraco, next fica em 2", t.receive(3) and t.next == 2)
check("2 fecha o buraco: next passa a 4", t.receive(2) and t.next == 4)
check("repetido e fora da janela", not t.receive(1) and not t.receive(4 + 1000))
t = SeqTracker(); t.next = 65535
check("dá a volta a 65535 -> 0", t.receive(65535) and t.next == 0 and t.receive(0))

print("[Subscrições]")
tab = SubscriptionTable()
tab.add(1, "north/*", 1); tab.add(1, "north/station7/*", 2); tab.add(2, "south/*", 1)
tab.add(3, "north/station7/temperature", 1)
m = tab.match("north/station7/temperature")
check("só os interessados", set(m) == {1, 3})
check("filtros sobrepostos: uma só entrada, qos mais forte", m[1] == 2)
check("ninguém interessado", tab.match("east/s1/humidity") == {})
tab.remove_session(1)
check("sessão removida deixa de receber", set(tab.match("north/station7/temperature")) == {3})

print()
sys.exit(1 if falhas else 0)