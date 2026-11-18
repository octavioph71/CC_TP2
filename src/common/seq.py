"""
Controlo do número de sequência no recetor (doc/protocolos.md, secção 3).

O recetor de cada troço guarda `next` (o SEQ que espera) e os SEQ que já chegaram à frente
(fora de ordem). SEQ é um u16 que dá a volta, por isso a distância usa módulo 2^16.
"""

MOD = 1 << 16
WINDOW = 32                


class SeqTracker:
    def __init__(self):
        self.next = 0
        self.ahead = set()

    def receive(self, seq):
        """True se `seq` é novo e está dentro da janela; False se é repetido ou está fora dela."""
        gap = (seq - self.next) % MOD
        if gap >= WINDOW or seq in self.ahead:
            return False                                
        if gap > 0:
            self.ahead.add(seq)                        
            return True
        self.next = (self.next + 1) % MOD                
        while self.next in self.ahead:                   
            self.ahead.discard(self.next)
            self.next = (self.next + 1) % MOD
        return True