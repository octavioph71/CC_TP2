"""
Ponto de entrada dos três papéis (enunciado, secção 5.2).

    ./run.sh broker     --id <id> --config <ficheiro>
    ./run.sh publisher  --id <id> --config <ficheiro>
    ./run.sh subscriber --id <id> --config <ficheiro>

O main lê os argumentos e a configuração, prepara a terminação ordenada
(SIGTERM/SIGINT) e passa o controlo ao módulo do papel. Cada papel expõe:

    run(node_id: str, cfg, stop: threading.Event) -> int

e é responsável por terminar sozinho (duration / drain) ou quando 'stop' for ativado.

Códigos de saída: 0 normal · 1 erro durante a execução · 2 erro de argumentos ou configuração
"""

from __future__ import annotations

import argparse
import signal
import sys
import threading
import traceback

from common.config import ConfigError, load_config

ROLES = ("broker", "publisher", "subscriber")


def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog="run.sh",
        description="Sistema de publicação/subscrição — TP2 de CC")
    parser.add_argument("role", choices=ROLES, help="papel do processo")
    parser.add_argument("--id", required=True, dest="node_id",
                        help="identificador do processo (ex.: broker1, pub1, sub1)")
    parser.add_argument("--config", required=True,
                        help="caminho absoluto do ficheiro de configuração")
    return parser.parse_args(argv)


def install_signal_handlers(stop: threading.Event) -> None:
    """
    1.º SIGTERM/SIGINT: pede uma terminação ordenada (o papel vê 'stop' e sai).
    2.º sinal:          sai de imediato.

    SIGSTOP/SIGCONT não se apanham: o SO suspende e retoma o processo sozinho.
    """
    def handler(signum, _frame):
        if stop.is_set():
            sys.stderr.write(f"[main] sinal {signum} repetido: a sair já\n")
            sys.stderr.flush()
            raise SystemExit(1)
        sys.stderr.write(f"[main] sinal {signum} recebido: a terminar\n")
        sys.stderr.flush()
        stop.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def load_role(role: str):
    """Importa só o módulo do papel pedido."""
    if role == "broker":
        from broker.broker import run
    elif role == "publisher":
        from publisher.publisher import run
    else:
        from subscriber.subscriber import run
    return run


def main(argv=None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)

    try:
        cfg = load_config(args.config, args.role)
    except ConfigError as e:
        print(f"[{args.node_id}] erro de configuração: {e}", file=sys.stderr)
        return 2

    stop = threading.Event()
    install_signal_handlers(stop)

    run = load_role(args.role)
    try:
        return run(args.node_id, cfg, stop) or 0
    except KeyboardInterrupt:
        return 0
    except Exception:
        print(f"[{args.node_id}] erro não tratado:", file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())