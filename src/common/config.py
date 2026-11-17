"""
Leitura do ficheiro de configuração (enunciado, secção 5.2).

Formato:
    - uma linha por chave:  chave = valor
    - '#' inicia um comentário até ao fim da linha
    - um valor é um primeiro termo obrigatório seguido de opções nome=valor
    - não há valores por omissão: todas as chaves obrigatórias têm de estar presentes

Uso:
    cfg = load_config("/caminho/absoluto/broker1.conf", "broker")
    cfg.control_port   # -> 7310
"""

from __future__ import annotations

import ipaddress
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from common.topics import is_valid_filter as _is_valid_filter  
from common.topics import is_valid_topic as _is_valid_topic    


class ConfigError(Exception):
    """Erro no ficheiro de configuração (inclui o número da linha, quando se aplica)."""

@dataclass(frozen=True)
class Endpoint:
    """Um par endereço:porta (ex.: broker_control, peer)."""
    host: str
    port: int

    def as_tuple(self) -> Tuple[str, int]:
        return (self.host, self.port)

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class PublishFlow:
    """Uma chave publish: <topico> qos=<1|2> count=<n> rate=<por segundo>."""
    topic: str
    qos: int
    count: int
    rate: float


@dataclass(frozen=True)
class Subscription:
    """Uma chave subscribe: <filtro> qos=<1|2>."""
    filter: str
    qos: int


@dataclass(frozen=True)
class TimedAction:
    """
    Uma chave at: <segundos> subscribe=<filtro> qos=<1|2>
               ou <segundos> unsubscribe=<filtro>
    'at' conta a partir do arranque do processo.
    """
    at: float
    action: str               # "subscribe" ou "unsubscribe"
    filter: str
    qos: Optional[int]       

@dataclass
class BrokerConfig:
    bind: str
    control_port: int
    data_port: int
    duration: float
    evict_after: float
    peers: List[Endpoint] = field(default_factory=list)

@dataclass
class PublisherConfig:
    broker_control: Endpoint
    log: str
    start_after: float
    drain: float
    publish: List[PublishFlow] = field(default_factory=list)

@dataclass
class SubscriberConfig:
    broker_control: Endpoint
    log: str
    duration: float
    subscribe: List[Subscription] = field(default_factory=list)
    at: List[TimedAction] = field(default_factory=list)


# Chaves que aparecem exatamente uma vez
SINGLE_KEYS: Dict[str, Tuple[str, ...]] = {
    "broker":     ("bind", "control_port", "data_port", "duration", "evict_after"),
    "publisher":  ("broker_control", "log", "start_after", "drain"),
    "subscriber": ("broker_control", "log", "duration"),
}

# Chaves que aparecem zero ou mais vezes 
LIST_KEYS: Dict[str, Tuple[str, ...]] = {
    "broker":     ("peer",),
    "publisher":  ("publish",),
    "subscriber": ("subscribe", "at"),
}

ROLES = tuple(SINGLE_KEYS.keys())

@dataclass(frozen=True)
class _RawValue:
    """Valor de uma linha já partido: primeiro termo + opções nome=valor."""
    first: str
    options: Dict[str, str]
    lineno: int


def _parse_lines(text: str) -> List[Tuple[str, _RawValue]]:
    """Devolve a lista (chave, valor) pela ordem em que aparecem no ficheiro."""
    entries: List[Tuple[str, _RawValue]] = []

    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.split("#", 1)[0].strip()      
        if not line:
            continue

        if "=" not in line:
            raise ConfigError(f"linha {lineno}: falta '=' em '{raw.strip()}'")

        key, value = line.split("=", 1)        
        key = key.strip()
        tokens = value.split()

        if not key:
            raise ConfigError(f"linha {lineno}: chave vazia")
        if not tokens:
            raise ConfigError(f"linha {lineno}: chave '{key}' sem valor")

        options: Dict[str, str] = {}
        for tok in tokens[1:]:
            if "=" not in tok:
                raise ConfigError(
                    f"linha {lineno}: opção '{tok}' não tem a forma nome=valor")
            name, val = tok.split("=", 1)
            if not name or not val:
                raise ConfigError(f"linha {lineno}: opção '{tok}' incompleta")
            if name in options:
                raise ConfigError(f"linha {lineno}: opção '{name}' repetida")
            options[name] = val

        entries.append((key, _RawValue(tokens[0], options, lineno)))

    return entries

def _err(rv: _RawValue, msg: str) -> ConfigError:
    return ConfigError(f"linha {rv.lineno}: {msg}")


def _no_options(key: str, rv: _RawValue) -> None:
    if rv.options:
        raise _err(rv, f"'{key}' não aceita opções ({', '.join(rv.options)})")


def _expect_options(key: str, rv: _RawValue, names: Tuple[str, ...]) -> None:
    """As opções de cada valor estão sempre todas presentes, e só essas."""
    missing = [n for n in names if n not in rv.options]
    extra = [n for n in rv.options if n not in names]
    if missing:
        raise _err(rv, f"'{key}' sem a(s) opção(ões): {', '.join(missing)}")
    if extra:
        raise _err(rv, f"'{key}' com opção(ões) desconhecida(s): {', '.join(extra)}")


def _ipv4(rv: _RawValue, text: str) -> str:
    try:
        return str(ipaddress.IPv4Address(text))
    except ValueError:
        raise _err(rv, f"endereço IPv4 inválido: '{text}'") from None


def _port(rv: _RawValue, text: str) -> int:
    try:
        port = int(text)
    except ValueError:
        raise _err(rv, f"porta inválida: '{text}'") from None
    if not 1 <= port <= 65535:
        raise _err(rv, f"porta fora do intervalo 1-65535: {port}")
    return port


def _endpoint(rv: _RawValue, text: str) -> Endpoint:
    host, sep, port = text.rpartition(":")
    if not sep:
        raise _err(rv, f"esperava endereço:porta, recebi '{text}'")
    return Endpoint(_ipv4(rv, host), _port(rv, port))


def _seconds(rv: _RawValue, text: str) -> float:
    try:
        value = float(text)
    except ValueError:
        raise _err(rv, f"número de segundos inválido: '{text}'") from None
    if value < 0:
        raise _err(rv, f"número de segundos negativo: {value}")
    return value


def _positive_int(rv: _RawValue, name: str, text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise _err(rv, f"{name} tem de ser inteiro: '{text}'") from None
    if value < 0:
        raise _err(rv, f"{name} não pode ser negativo: {value}")
    return value


def _positive_float(rv: _RawValue, name: str, text: str) -> float:
    try:
        value = float(text)
    except ValueError:
        raise _err(rv, f"{name} tem de ser numérico: '{text}'") from None
    if value <= 0:
        raise _err(rv, f"{name} tem de ser > 0: {value}")
    return value


def _qos(rv: _RawValue, text: str) -> int:
    if text not in ("1", "2"):
        raise _err(rv, f"qos tem de ser 1 ou 2, recebi '{text}'")
    return int(text)


def _abs_path(rv: _RawValue, text: str) -> str:
    if not os.path.isabs(text):
        raise _err(rv, f"o caminho tem de ser absoluto: '{text}'")
    return text


def _topic(rv: _RawValue, text: str) -> str:
    if not _is_valid_topic(text):
        raise _err(rv, f"tópico inválido (regiao/estacao/metrica): '{text}'")
    return text


def _filter(rv: _RawValue, text: str) -> str:
    if not _is_valid_filter(text):
        raise _err(rv, f"filtro inválido: '{text}'")
    return text

def _convert(key: str, rv: _RawValue):
    """Converte o valor de uma chave para o tipo certo."""

    if key == "bind":
        _no_options(key, rv)
        return _ipv4(rv, rv.first)

    if key in ("control_port", "data_port"):
        _no_options(key, rv)
        return _port(rv, rv.first)

    if key in ("broker_control", "peer"):
        _no_options(key, rv)
        return _endpoint(rv, rv.first)

    if key == "log":
        _no_options(key, rv)
        return _abs_path(rv, rv.first)

    if key in ("duration", "start_after", "drain", "evict_after"):
        _no_options(key, rv)
        return _seconds(rv, rv.first)

    if key == "publish":
        _expect_options(key, rv, ("qos", "count", "rate"))
        return PublishFlow(
            topic=_topic(rv, rv.first),
            qos=_qos(rv, rv.options["qos"]),
            count=_positive_int(rv, "count", rv.options["count"]),
            rate=_positive_float(rv, "rate", rv.options["rate"]),
        )

    if key == "subscribe":
        _expect_options(key, rv, ("qos",))
        return Subscription(
            filter=_filter(rv, rv.first),
            qos=_qos(rv, rv.options["qos"]),
        )

    if key == "at":
        when = _seconds(rv, rv.first)
        if "subscribe" in rv.options:
            _expect_options(key, rv, ("subscribe", "qos"))
            return TimedAction(when, "subscribe",
                               _filter(rv, rv.options["subscribe"]),
                               _qos(rv, rv.options["qos"]))
        if "unsubscribe" in rv.options:
            _expect_options(key, rv, ("unsubscribe",))
            return TimedAction(when, "unsubscribe",
                               _filter(rv, rv.options["unsubscribe"]),
                               None)
        raise _err(rv, "'at' precisa de subscribe=<filtro> qos=<n> "
                       "ou unsubscribe=<filtro>")

    raise _err(rv, f"chave sem conversor: '{key}'")

def load_config(path: str, role: str):
    """
    Lê e valida a configuração de um papel.

    Devolve BrokerConfig, PublisherConfig ou SubscriberConfig.
    Lança ConfigError se faltar uma chave obrigatória, se uma chave única
    aparecer repetida, ou se algum valor for inválido.
    """
    if role not in ROLES:
        raise ConfigError(f"papel desconhecido: '{role}'")

    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        raise ConfigError(f"não consegui ler '{path}': {e.strerror}") from None

    single_keys = SINGLE_KEYS[role]
    list_keys = LIST_KEYS[role]

    single: Dict[str, object] = {}
    lists: Dict[str, list] = {k: [] for k in list_keys}

    for key, rv in _parse_lines(text):
        if key in single_keys:
            if key in single:
                raise _err(rv, f"chave '{key}' repetida (só pode aparecer uma vez)")
            single[key] = _convert(key, rv)
        elif key in list_keys:
            lists[key].append(_convert(key, rv))
        else:
            print(f"[config] aviso: linha {rv.lineno}: chave '{key}' "
                  f"ignorada no papel '{role}'", file=sys.stderr)

    missing = [k for k in single_keys if k not in single]
    if missing:
        raise ConfigError(f"falta(m) a(s) chave(s) obrigatória(s): {', '.join(missing)}")

    if role == "broker":
        return BrokerConfig(peers=lists["peer"], **single)
    if role == "publisher":
        return PublisherConfig(publish=lists["publish"], **single)
    return SubscriberConfig(subscribe=lists["subscribe"], at=lists["at"], **single)