"""Minimal Prometheus text-format parser (exporters: kafka-exporter, postgres_exporter, nginx, envoy...)."""
import math
import re

_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+(\S+)')
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:[^"\\]|\\.)*)"')


def _labels(raw: str) -> dict[str, str]:
    return {k: v.replace('\\"', '"').replace("\\\\", "\\").replace("\\n", "\n") for k, v in _LABEL.findall(raw or "")}


def parse(text: str) -> list[tuple[str, dict[str, str], float]]:
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _LINE.match(line)
        if not m:
            continue
        try:
            value = float(m.group(4))
        except ValueError:
            continue
        if math.isnan(value):
            continue
        out.append((m.group(1), _labels(m.group(3)), value))
    return out


def parse_selector(selector: str) -> tuple[str, dict[str, str]]:
    """'kafka_consumergroup_lag{consumergroup="orders"}' -> (name, {labels})."""
    selector = selector.strip()
    if "{" in selector:
        name, rest = selector.split("{", 1)
        return name.strip(), _labels(rest.rstrip("}"))
    return selector, {}


def select(samples, name: str, labels: dict[str, str]) -> list[float]:
    return [v for n, lbl, v in samples if n == name and all(lbl.get(k) == want for k, want in labels.items())]


def aggregate(values: list[float], how: str = "max") -> float:
    if how == "sum":
        return sum(values)
    if how == "min":
        return min(values)
    if how == "avg":
        return sum(values) / len(values)
    return max(values)
