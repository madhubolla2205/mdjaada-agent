"""Work the platform requests during incidents. Queries run against the local store and return
compact, capped results: grouped errors plus a small sample of raw spans and log lines."""
import re
from datetime import datetime, timezone
from typing import Optional

from . import checks, otelquery
from .store import KIND_CLIENT, KIND_CONSUMER, KIND_PRODUCER, STATUS_ERROR, Store, now_ns

MAX_SPAN_SAMPLES = 25
MAX_LOG_SAMPLES = 50
MAX_SEARCH_ROWS = 500
MAX_SEARCH_HOURS = 72  # the store keeps 72 hours of spans and logs
_NOISE = [
    (re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I), "<uuid>"),
    (re.compile(r"\b[0-9a-f]{16,}\b", re.I), "<hex>"),
    (re.compile(r"\b\d{1,3}(\.\d{1,3}){3}(:\d+)?\b"), "<ip>"),
    (re.compile(r"(?<![a-zA-Z_])\d+(\.\d+)?"), "<n>"),
]


def normalize(text: str) -> str:
    """Strip ids and numbers so the same error groups together."""
    text = (text or "").strip()
    for pattern, repl in _NOISE:
        text = pattern.sub(repl, text)
    return text[:200]


def _ns(iso: Optional[str]) -> int:
    if not iso:
        return now_ns()
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1e9)


def _iso(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, timezone.utc).isoformat().replace("+00:00", "Z")


def _p95(values: list[float]) -> Optional[float]:
    if not values:
        return None
    values = sorted(values)
    return round(values[min(len(values) - 1, int(len(values) * 0.95))], 1)


def _span_out(s: dict) -> dict:
    return {"trace_id": s["trace_id"], "span_id": s["span_id"], "parent_span_id": s["parent_span_id"],
            "service": s["service"], "name": s["name"], "peer": s["peer"], "kind": s["kind"],
            "start": _iso(s["start_ns"]), "duration_ms": round(s["duration_ms"] or 0, 1),
            "error": s["status_code"] == STATUS_ERROR, "message": s["status_message"]}


def _log_out(r: dict) -> dict:
    return {"time": _iso(r["time_ns"]), "service": r["service"], "severity": r["severity_text"] or str(
            r["severity_number"]), "body": (r["body"] or "")[:2000], "trace_id": r["trace_id"],
            "span_id": r["span_id"]}


def evidence(store: Store, payload: dict) -> dict:
    services = payload.get("services") or None
    start, end = _ns(payload.get("start")), _ns(payload.get("end"))
    base_start = start - (end - start)

    stats: dict[str, dict] = {}
    for label, (a, b) in (("window", (start, end)), ("baseline", (base_start, start))):
        per: dict[str, list] = {}
        for row in store.span_durations(a, b, services):
            per.setdefault(row["service"], []).append(row)
        for svc, rows in per.items():
            s = stats.setdefault(svc, {"spans": 0, "error_spans": 0, "error_logs": 0})
            if label == "window":
                s["spans"] = len(rows)
                s["error_spans"] = sum(1 for r in rows if r["status_code"] == STATUS_ERROR)
                s["p95_ms"] = _p95([r["duration_ms"] for r in rows])
            else:
                s["baseline_p95_ms"] = _p95([r["duration_ms"] for r in rows])

    err_spans = store.error_spans(start, end, services)
    groups: dict[tuple, dict] = {}
    for s in err_spans:
        key = (s["service"], s["name"], normalize(s["status_message"]))
        g = groups.setdefault(key, {"service": key[0], "name": key[1], "message": key[2], "count": 0,
                                    "first": s["start_ns"], "last": s["start_ns"], "trace_ids": []})
        g["count"] += 1
        g["first"], g["last"] = min(g["first"], s["start_ns"]), max(g["last"], s["start_ns"])
        if len(g["trace_ids"]) < 3 and s["trace_id"] not in g["trace_ids"]:
            g["trace_ids"].append(s["trace_id"])
        if s["peer"] and services and s["peer"] in services and s["peer"] != s["service"]:
            # caller saw errors talking to an uninstrumented dependency: credit the dependency too
            stats.setdefault(s["peer"], {"spans": 0, "error_spans": 0, "error_logs": 0})["error_spans"] += 1

    trace_ids = list(dict.fromkeys(s["trace_id"] for s in err_spans if s["trace_id"]))
    logs = store.problem_logs(start, end, services, trace_ids)
    log_groups: dict[tuple, dict] = {}
    for r in logs:
        severe = r["severity_number"] >= 13 or (r["severity_text"] or "") in ("WARN", "WARNING", "ERROR", "FATAL", "CRITICAL")
        if r["severity_number"] >= 17 or (r["severity_text"] or "") in ("ERROR", "FATAL", "CRITICAL"):
            stats.setdefault(r["service"], {"spans": 0, "error_spans": 0, "error_logs": 0})["error_logs"] += 1
        if not severe:
            continue  # info lines linked by trace id are shown raw, not grouped as errors
        key = (r["service"], normalize(r["body"]))
        g = log_groups.setdefault(key, {"service": key[0], "message": key[1], "severity": r["severity_text"],
                                        "count": 0, "first": r["time_ns"], "last": r["time_ns"],
                                        "trace_ids": []})
        g["count"] += 1
        g["first"], g["last"] = min(g["first"], r["time_ns"]), max(g["last"], r["time_ns"])
        if r["trace_id"] and len(g["trace_ids"]) < 3 and r["trace_id"] not in g["trace_ids"]:
            g["trace_ids"].append(r["trace_id"])

    def top(gs):
        out = sorted(gs.values(), key=lambda g: g["count"], reverse=True)[:20]
        for g in out:
            g["first"], g["last"] = _iso(g["first"]), _iso(g["last"])
        return out

    return {
        "window": {"start": _iso(start), "end": _iso(end)},
        "services": stats,
        "error_groups": top(groups),
        "log_groups": top(log_groups),
        "spans": [_span_out(s) for s in err_spans[:MAX_SPAN_SAMPLES]],
        "logs": [_log_out(r) for r in logs[:MAX_LOG_SAMPLES]],
        "note": "" if (stats or logs) else "No spans or logs in the agent's store for this window; "
                                            "ranking uses check results and change events only.",
    }


def trace(store: Store, payload: dict) -> dict:
    spans, logs = store.trace(str(payload.get("trace_id", "")).lower())
    return {"trace_id": payload.get("trace_id"), "spans": [_span_out(s) for s in spans],
            "logs": [_log_out(r) for r in logs]}


def test_node(payload: dict) -> dict:
    r = checks.run_check({"node_id": "test", "type": payload.get("type", ""), "config": payload.get("config", {})})
    if r is None:
        return {"ok": False, "warn": False, "message": "nothing to check: set host+port, a URL or type settings"}
    return {"ok": r.ok, "warn": r.warn, "message": r.message, "latency_ms": r.latency_ms, "metrics": r.metrics}


def topology(store: Store, since_ns: int) -> list[dict]:
    """Service pairs oriented upstream -> downstream (the direction failures propagate)."""
    pairs: dict[tuple, dict] = {}

    def add(src, dst, relation, n, last):
        if not src or not dst or src == dst:
            return
        p = pairs.setdefault((src, dst), {"source": src, "target": dst, "relation": relation, "count": 0, "last": 0})
        p["count"] += n
        p["last"] = max(p["last"], last)

    for e in store.service_edges(since_ns):
        if e["kind"] == KIND_CONSUMER:
            if e["peer"]:
                continue  # the topic is known: drawn as producer -> topic -> consumer via peer_edges
            add(e["parent"], e["child"], "feeds", e["n"], e["last"])  # producer -> consumer via a broker
        else:  # caller depends on callee
            add(e["child"], e["parent"], "serves", e["n"], e["last"])
    for e in store.peer_edges(since_ns):
        if e["kind"] == KIND_PRODUCER:
            add(e["service"], e["peer"], "publishes_to", e["n"], e["last"])
        elif e["kind"] == KIND_CONSUMER:
            add(e["peer"], e["service"], "consumed_by", e["n"], e["last"])
        elif e["kind"] == KIND_CLIENT:
            add(e["peer"], e["service"], "serves", e["n"], e["last"])
    return [{"source": p["source"], "target": p["target"], "relation": p["relation"], "count": p["count"],
             "last_seen": _iso(p["last"])} for p in pairs.values()]


def search(store: Store, payload: dict) -> dict:
    """Explore: spans, logs or the list of services, filtered as the user asked; capped and newest-window only."""
    what = payload.get("what")
    end = _ns(payload.get("end")) if payload.get("end") else now_ns()
    if payload.get("start"):
        start = _ns(payload.get("start"))
    else:
        start = end - int(float(payload.get("minutes") or 60) * 60e9)
    start = max(start, end - int(MAX_SEARCH_HOURS * 3600e9))
    limit = max(1, min(int(payload.get("limit") or 200), MAX_SEARCH_ROWS))
    text = lambda k: (str(payload.get(k) or "").strip()[:200] or None)  # noqa: E731
    window = {"start": _iso(start), "end": _iso(end)}
    if what == "spans":
        min_ms = payload.get("min_ms")
        rows = store.search_spans(start, end, service=text("service"), name=text("name"),
                                  errors_only=bool(payload.get("errors_only")),
                                  min_ms=float(min_ms) if min_ms not in (None, "") else None,
                                  sort="slowest" if payload.get("sort") != "newest" else "newest", limit=limit)
        return {"what": "spans", "window": window, "spans": [_span_out(r) for r in rows], "limit": limit,
                "sort": "newest" if payload.get("sort") == "newest" else "slowest"}
    if what == "logs":
        sev = payload.get("severity") if payload.get("severity") in ("warn", "error") else None
        rows = store.search_logs(start, end, service=text("service"), text=text("text"), severity=sev, limit=limit)
        return {"what": "logs", "window": window, "logs": [_log_out(r) for r in rows], "limit": limit}
    if what == "services":
        return {"what": "services", "window": window, "services": store.services(start)}
    if what == "metric_catalog":
        since = now_ns() - int(min(float(payload.get("minutes") or 60), otelquery.MAX_MINUTES) * 60e9)
        return {"what": "metric_catalog", "metrics": otelquery.catalog(store, since)}
    if what == "metric_series":
        return otelquery.series(store, payload)
    raise ValueError(f"unknown search {what!r}")


def run(store: Store, kind: str, payload: dict) -> dict:
    if kind == "evidence":
        return evidence(store, payload)
    if kind == "trace":
        return trace(store, payload)
    if kind == "test_node":
        return test_node(payload)
    if kind == "search":
        return search(store, payload)
    if kind == "metric_names":  # OTel metrics received in the last hour, for the node settings picker
        return {"metrics": store.metric_names(now_ns() - int(float(payload.get("minutes", 60)) * 60e9))}
    raise ValueError(f"unknown task kind {kind!r}")
