import random
import socket
import sys
import time

from common.client import SessionError, open_session
from common.log import EventLog
from protocol import eventcast as ec
from protocol import subcontrol as sc

LINGER = 0.5    

"""
Publicador, fase 1: abre a sessão TCP, regista os fluxos, espera start_after e publica por UDP
à cadência pedida, escrevendo uma linha no registo por publicação.
"""

def register(sock, flows):
    """Um REGISTER por fluxo. Devolve [TOPIC_ID], na ordem dos fluxos."""
    ids = []
    for f in flows:
        try:
            sc.send_msg(sock, sc.Register(f.qos, f.topic))
            ack = sc.recv_msg(sock)
        except (OSError, sc.ConnectionClosed, sc.ProtocolError) as e:
            raise SessionError(f"sem resposta ao REGISTER de {f.topic}: {e}") from None
        if not isinstance(ack, sc.RegisterAck) or ack.status != sc.ST_OK:
            raise SessionError(f"REGISTER de {f.topic} recusado: {ack}")
        ids.append(ack.topic_id)
    return ids


def run(node_id, cfg, stop):
    usock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    usock.bind(("0.0.0.0", 0))
    try:
        sock, ack = open_session(cfg.broker_control, sc.ROLE_PUBLISHER, node_id,
                                 usock.getsockname()[1], stop)
        topic_ids = register(sock, cfg.publish)
    except SessionError as e:
        print(f"[{node_id}] {e}", file=sys.stderr)
        return 1

    data_addr = (cfg.broker_control.host, ack.data_port)       
    print(f"[{node_id}] sessão {ack.session}; dados em {data_addr[0]}:{data_addr[1]}", flush=True)

    start = time.monotonic() + cfg.start_after
    events = sorted((start + n / f.rate, i, n + 1) for i, f in enumerate(cfg.publish) for n in range(f.count))

    log = EventLog(cfg.log, node_id)
    rng = random.Random(node_id)
    seq = 0
    try:
        for due, i, msg_id in events:
            if stop.wait(max(0.0, due - time.monotonic())):
                break
            f = cfg.publish[i]
            payload = f"{rng.uniform(10, 40):.1f}".encode()
            usock.sendto(ec.encode(ec.Publish(ack.session, seq, msg_id, topic_ids[i], f.qos, payload)), data_addr)
            seq = (seq + 1) % 0x10000
            log.write(f.topic, msg_id)
        stop.wait(LINGER)
    finally:
        try:
            sc.send_msg(sock, sc.Bye(sc.BYE_NORMAL))
        except OSError:
            pass
        sock.close()
        usock.close()
        log.close()
    print(f"[{node_id}] fim ({seq} publicações)", flush=True)
    return 0