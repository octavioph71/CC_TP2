import socket
import sys
import time

from common.client import SessionError, open_session
from common.log import EventLog
from common.seq import NEW, SeqTracker
from protocol import eventcast as ec
from protocol import subcontrol as sc

"""
Subscritor, fase 1: abre a sessão TCP, subscreve os filtros do arranque, recebe DELIVER por UDP e escreve uma linha no registo por publicação entregue.
"""

def subscribe(sock, subs):
    """Um SUBSCRIBE por filtro, espera pela resposta de cada um."""
    sock.settimeout(5.0)
    try:
        for sub in subs:
            sc.send_msg(sock, sc.Subscribe(sub.qos, sub.filter))
            ack = sc.recv_msg(sock)
            if not isinstance(ack, sc.SubAck) or ack.status != sc.ST_OK:
                raise SessionError(f"SUBSCRIBE de {sub.filter} recusado: {ack}")
    except (OSError, sc.ConnectionClosed, sc.ProtocolError) as e:
        raise SessionError(f"sem resposta ao SUBSCRIBE: {e}") from None
    sock.settimeout(None)


def run(node_id, cfg, stop):
    end = time.monotonic() + cfg.duration
    usock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    usock.bind(("0.0.0.0", 0))
    try:
        sock, ack = open_session(cfg.broker_control, sc.ROLE_SUBSCRIBER, node_id,
                                 usock.getsockname()[1], stop)
        subscribe(sock, cfg.subscribe)
    except SessionError as e:
        print(f"[{node_id}] {e}", file=sys.stderr)
        return 1

    broker_ip = cfg.broker_control.host
    print(f"[{node_id}] sessão {ack.session}; dados em {broker_ip}:{ack.data_port}", flush=True)

    log = EventLog(cfg.log, node_id)
    tracker = SeqTracker()
    received = 0
    usock.settimeout(0.2)
    try:
        while not stop.is_set() and time.monotonic() < end:
            try:
                data, addr = usock.recvfrom(ec.RECV_BUFSIZE)
            except (socket.timeout, OSError):
                continue
            if addr[0] != broker_ip:
                continue
            try:
                msg = ec.decode(data)
            except ec.DecodeError:
                continue
            if not isinstance(msg, ec.Deliver) or msg.session != ack.session:
                continue                                   

            expected = tracker.next
            if tracker.receive(msg.seq) != NEW:
                print(f"[{node_id}] SEQ {msg.seq} repetido ou fora da janela: descartado", flush=True)
                continue
            if msg.seq != expected:
                print(f"[{node_id}] esperava SEQ {expected}, chegou {msg.seq} (perda ou fora de ordem)", flush=True)
            log.write(msg.topic, msg.msg_id)
            received += 1
    finally:
        try:
            sc.send_msg(sock, sc.Bye(sc.BYE_NORMAL))
        except OSError:
            pass
        sock.close()
        usock.close()
        log.close()
    print(f"[{node_id}] fim ({received} entregas)", flush=True)
    return 0