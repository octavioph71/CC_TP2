"""
Controlo do número de sequência no recetor (doc/protocolos.md, secção 4).

O recetor de cada troço guarda `next` (o SEQ que espera) e o conjunto dos SEQ que já chegaram
à frente de `next` (fora de ordem). SEQ é um u16 que dá a volta, por isso "à frente" e "atrás"
usam aritmética de números de série (RFC 1982).
"""

from __future__ import annotations

MOD = 1 << 16
HALF = 1 << 15
WINDOW = 32                

NEW = "new"                  # visto pela primeira vez 
DUP = "dup"                  # já recebido
FAR = "far"                  # longe de mais para ser de um emissor correto


class SeqTracker:
    def __init__(self) -> None:
        self.next = 0
        self._ahead = set()

    def receive(self, seq: int) -> str:
        gap = (seq - self.next) % MOD
        if gap == 0:                                    
            self.next = (self.next + 1) % MOD
            while self.next in self._ahead:              
                self._ahead.discard(self.next)
                self.next = (self.next + 1) % MOD
            return NEW
        if gap < WINDOW:                              
            if seq in self._ahead:
                return DUP
            self._ahead.add(seq)
            return NEW
        return FAR if gap < HALF else DUP              