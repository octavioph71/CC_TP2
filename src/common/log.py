"""
Registo de publicações e de entregas (enunciado, secção 5.2, "Registos").

Formato, uma linha por publicação (publicador) ou por entrega (subscritor):

    <timestamp_ms> <id> <topico> <message_id>

- timestamp_ms: tempo Unix em milissegundos (relógio do sistema, não monótono)
- id:           o --id com que o processo foi lançado
- message_id:   o MSG_ID em decimal; a mesma cadeia no publicador e no subscritor

Cada linha é enviada para o ficheiro no momento em que é escrita (flush),
para que nenhuma forma de terminar a perca, SIGKILL incluído.

Uso:
    log = EventLog(cfg.log, node_id)
    log.write("north/station7/temperature", 17)
    ...
    log.close()
"""

from __future__ import annotations

import threading
import time


def now_ms() -> int:
    """Tempo Unix em milissegundos, do relógio do sistema."""
    return time.time_ns() // 1_000_000


class EventLog:
    """Ficheiro de registo de um publicador ou de um subscritor. Seguro entre threads."""

    def __init__(self, path: str, node_id: str) -> None:
        if not node_id or any(c.isspace() for c in node_id):
            raise ValueError(f"id inválido para o registo: '{node_id}'")

        self._node_id = node_id
        self._lock = threading.Lock()
        # "w": cada execução começa com o ficheiro vazio.
        # buffering=1: buffer de linha (texto); o flush explícito abaixo garante-o na mesma.
        self._file = open(path, "w", encoding="utf-8", buffering=1)

    def write(self, topic: str, message_id: int) -> None:
        """Escreve uma linha e passa-a logo ao sistema operativo."""
        line = f"{now_ms()} {self._node_id} {topic} {message_id}\n"
        with self._lock:
            if self._file.closed:
                return                      # a terminar: ignora escritas tardias
            self._file.write(line)
            # flush entrega a linha ao SO; a partir daí sobrevive a um SIGKILL
            # do processo (só se perderia se a máquina inteira fosse abaixo).
            self._file.flush()

    def close(self) -> None:
        with self._lock:
            if not self._file.closed:
                self._file.close()

    def __enter__(self) -> "EventLog":
        return self

    def __exit__(self, *exc) -> None:
        self.close()