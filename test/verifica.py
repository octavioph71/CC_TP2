"""
Verificação rápida dos módulos já feitos (config, log, subcontrol, eventcast).

    python3 test/verificar.py          (a partir da raiz do projeto)
"""
import os
import socket
import sys
import tempfile

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "src"))

from common.config import load_config, ConfigError
from common.log import EventLog
from protocol import subcontrol as sc
from protocol import eventcast as ec

falhas = 0


def check(nome, condicao):
    global falhas
    print(("  OK    " if condicao else "  FALHA ") + nome)
    if not condicao:
        falhas += 1


def erro(nome, excecao, funcao):
    """Verifica que funcao() lança a exceção esperada."""
    try:
        funcao()
        check(nome, False)
    except excecao:
        check(nome, True)


# --------------------------------------------------------------------------- #
print("\n[1] config.py")
conf = os.path.join(RAIZ, "config", "local")
b = load_config(os.path.join(conf, "broker1.conf"), "broker")
p = load_config(os.path.join(conf, "pub1.conf"), "publisher")
s = load_config(os.path.join(conf, "sub1.conf"), "subscriber")
check("broker1.conf lido (portas 7310/7311)", (b.control_port, b.data_port) == (7310, 7311))
check("pub1.conf lido (1 fluxo, qos=1)", len(p.publish) == 1 and p.publish[0].qos == 1)
check("sub1.conf lido (filtro north/station7/*)", s.subscribe[0].filter == "north/station7/*")

with tempfile.NamedTemporaryFile("w", suffix=".conf", delete=False) as f:
    f.write("bind = 127.0.0.1\n")
erro("chave obrigatória em falta -> ConfigError", ConfigError,
     lambda: load_config(f.name, "broker"))
with open(f.name, "w") as g:
    g.write("broker_control = 1.2.3.4:5\nlog = /tmp/x\nduration = 1\n"
            "subscribe = north/*/temp qos=1\n")
erro("filtro inválido (north/*/temp) -> ConfigError", ConfigError,
     lambda: load_config(f.name, "subscriber"))
os.unlink(f.name)

# --------------------------------------------------------------------------- #
print("\n[2] log.py")
caminho = os.path.join(tempfile.gettempdir(), "verificar_tp2.log")
log = EventLog(caminho, "sub1")
log.write("north/station7/temperature", 17)
log.close()
linha = open(caminho).read().split()
check("4 campos por linha", len(linha) == 4)
check("timestamp em ms (13 dígitos)", linha[0].isdigit() and len(linha[0]) == 13)
check("id, tópico e message_id certos",
      linha[1:] == ["sub1", "north/station7/temperature", "17"])
os.unlink(caminho)

# --------------------------------------------------------------------------- #
print("\n[3] subcontrol.py")
msgs = [sc.Hello(sc.ROLE_PUBLISHER, 40001, "pub1"), sc.HelloAck(sc.ST_OK, 7311, 1),
        sc.Register(1, "north/station7/temperature"), sc.RegisterAck(sc.ST_OK, 1),
        sc.Subscribe(1, 2, "north/station7/*"), sc.SubAck(1, sc.ST_OK),
        sc.Unsubscribe(2, "north/*"), sc.UnsubAck(2, sc.ST_OK), sc.Bye(sc.BYE_NORMAL)]
check("HELLO com os bytes do protocolos.md",
      sc.encode(msgs[0]).hex(" ") == "01 00 08 01 9c 41 04 70 75 62 31")

a, bsock = socket.socketpair()
ca, cb = sc.Channel(a), sc.Channel(bsock)
for m in msgs:
    ca.send(m)
check("9 tipos de mensagem: ida e volta", all(cb.recv() == m for m in msgs))

a.sendall(b"".join(sc.encode(m) for m in msgs))
check("mensagens coladas num só envio", all(cb.recv() == m for m in msgs))

a.sendall(bytes([0x7F, 0, 0]))
erro("tipo desconhecido -> ProtocolError", sc.ProtocolError, cb.recv)
a.close()
erro("ligação fechada -> ConnectionClosed", sc.ConnectionClosed, cb.recv)
bsock.close()

# --------------------------------------------------------------------------- #
print("\n[4] eventcast.py")
pub = ec.Publish(1, 0, 1, 1, 1, b"23.5")
dlv = ec.Deliver(2, 0, 1, "north/station7/temperature", 1, b"23.5")
ack = ec.Ack(1, 0, 1)
check("PUBLISH: 17 B e ida e volta", len(ec.encode(pub)) == 17 and ec.decode(ec.encode(pub)) == pub)
check("DELIVER: 42 B e ida e volta", len(ec.encode(dlv)) == 42 and ec.decode(ec.encode(dlv)) == dlv)
check("ACK: 7 B e ida e volta", len(ec.encode(ack)) == 7 and ec.decode(ec.encode(ack)) == ack)
check("mark_retx liga o bit RTX", ec.decode(ec.mark_retx(ec.encode(pub))).retx)

rejeitados = 0
total = 0
for m in (pub, dlv, ack):
    dados = ec.encode(m)
    for n in range(len(dados)):
        total += 1
        try:
            ec.decode(dados[:n])
        except ec.DecodeError:
            rejeitados += 1
check(f"datagramas truncados rejeitados ({rejeitados}/{total})", rejeitados == total)

u1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
u1.bind(("127.0.0.1", 0))
u2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
u2.sendto(ec.encode(dlv), u1.getsockname())
dados, _ = u1.recvfrom(ec.RECV_BUFSIZE)
check("DELIVER enviado por UDP chega igual", ec.decode(dados) == dlv)
u1.close()
u2.close()

# --------------------------------------------------------------------------- #
print()
if falhas:
    print(f"{falhas} verificação(ões) falharam.")
    sys.exit(1)
print("Tudo OK.")