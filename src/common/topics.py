"""
Tópicos, filtros e tabela de subscrições (enunciado, secção 3.4).

Um tópico tem sempre 3 níveis (regiao/estacao/metrica). Um filtro é um tópico completo
ou um prefixo terminado em '/*'. Por isso um tópico r/s/m só pode ser correspondido por
três filtros: r/s/m, r/s/* e r/*. A tabela aproveita isso: para encontrar os interessados
faz 3 consultas a um dicionário, em vez de percorrer todas as subscrições.
"""

from __future__ import annotations

from typing import Dict, Set, Tuple


def is_valid_topic(topic: str) -> bool:
    """regiao/estacao/metrica: exatamente 3 níveis, não vazios, sem '*'."""
    levels = topic.split("/")
    return len(levels) == 3 and all(lvl and "*" not in lvl for lvl in levels)


def is_valid_filter(flt: str) -> bool:
    """Um tópico completo, ou um prefixo de 1 ou 2 níveis terminado em '/*'."""
    if is_valid_topic(flt):
        return True
    levels = flt.split("/")
    return (2 <= len(levels) <= 3 and levels[-1] == "*"
            and all(lvl and "*" not in lvl for lvl in levels[:-1]))


def candidate_filters(topic: str) -> Tuple[str, str, str]:
    """Os únicos 3 filtros que podem corresponder a este tópico."""
    region, station, _ = topic.split("/")
    return topic, f"{region}/{station}/*", f"{region}/*"


class SubscriptionTable:
    """
    filtro -> {sessão -> qos}, mais o índice inverso sessão -> filtros.
    Não tem lock: quem a usa (o broker) protege-a.
    """

    def __init__(self) -> None:
        self._by_filter: Dict[str, Dict[int, int]] = {}
        self._by_session: Dict[int, Set[str]] = {}

    def add(self, session: int, flt: str, qos: int) -> None:
        """Subscreve (ou muda o qos de uma subscrição que já existia)."""
        self._by_filter.setdefault(flt, {})[session] = qos
        self._by_session.setdefault(session, set()).add(flt)

    def remove(self, session: int, flt: str) -> bool:
        """Cancela. False se a sessão não tinha esse filtro."""
        subs = self._by_filter.get(flt)
        if not subs or session not in subs:
            return False
        del subs[session]
        if not subs:
            del self._by_filter[flt]
        self._by_session[session].discard(flt)
        return True

    def remove_session(self, session: int) -> None:
        for flt in list(self._by_session.get(session, ())):
            self.remove(session, flt)
        self._by_session.pop(session, None)

    def match(self, topic: str) -> Dict[int, int]:
        """
        Sessão -> qos para cada sessão interessada. Uma sessão aparece UMA vez,
        mesmo que vários filtros dela correspondam; com qos diferentes vale o mais forte.
        """
        result: Dict[int, int] = {}
        for flt in candidate_filters(topic):
            for session, qos in self._by_filter.get(flt, {}).items():
                if qos > result.get(session, 0):
                    result[session] = qos
        return result