"""
Ponta a ponta com o run.sh, a partir de outra diretoria: um broker, um publicador e dois
subscritores (um com filtro que corresponde, outro que não).     python3 test/e2e.py
"""
import os
import subprocess
import sys
import tempfile
import time

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = os.path.join(RAIZ, "run.sh")
T, H = "north/station7/temperature", "north/station7/humidity"

d = tempfile.mkdtemp(prefix="tp2_")


def conf(nome, texto):
    p = os.path.join(d, nome + ".conf")
    open(p, "w").write(texto)
    return p


cfgs = {
    "broker": conf("b", "bind=127.0.0.1\ncontrol_port=7510\ndata_port=7511\nduration=9\nevict_after=5\n"),
    "pub1": conf("p", f"broker_control=127.0.0.1:7510\nlog={d}/pub1.log\nstart_after=1\ndrain=1\n"
                      f"publish = {T} qos=1 count=40 rate=20\npublish = {H} qos=2 count=20 rate=10\n"),
    "sub1": conf("s1", f"broker_control=127.0.0.1:7510\nlog={d}/sub1.log\nduration=7\n"
                       "subscribe = north/station7/* qos=1\nsubscribe = north/* qos=1\n"),
    "sub2": conf("s2", f"broker_control=127.0.0.1:7510\nlog={d}/sub2.log\nduration=7\nsubscribe = south/* qos=1\n"),
}
papeis = {"broker": "broker", "pub1": "publisher", "sub1": "subscriber", "sub2": "subscriber"}
ids = {"broker": "broker1", "pub1": "pub1", "sub1": "sub1", "sub2": "sub2"}

procs = []
for nome in ("broker", "sub1", "sub2", "pub1"):
    procs.append(subprocess.Popen([RUN, papeis[nome], "--id", ids[nome], "--config", cfgs[nome]],
                                  cwd="/", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    if nome == "broker":
        time.sleep(0.3)
codigos = [p.wait(timeout=30) for p in procs]


def ler(nome):
    return [l.split() for l in open(os.path.join(d, nome + ".log"))]


falhas = 0


def check(nome, ok):
    global falhas
    print(("  OK    " if ok else "  FALHA ") + nome)
    falhas += not ok


pub, s1, s2 = ler("pub1"), ler("sub1"), ler("sub2")
check("todos os processos terminaram com 0", all(c == 0 for c in codigos))
check("publicador registou 60", len(pub) == 60)
check("sub1 recebeu as 60, cada uma uma vez (filtros sobrepostos)",
      sorted((l[2], l[3]) for l in s1) == sorted((l[2], l[3]) for l in pub))
check("sub2 (outro filtro) não recebeu nada", s2 == [])
check("registo no formato <ms> <id> <tópico> <id>", all(len(l) == 4 and l[0].isdigit() for l in s1 + pub))
sys.exit(1 if falhas else 0)