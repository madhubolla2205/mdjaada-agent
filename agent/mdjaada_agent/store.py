"""Telemetry store inside the customer's environment. Spans, logs and OTel metric points are written here and never
leave it; the platform only receives query results it asks for during an incident.

This implementation uses SQLite so the agent runs with zero dependencies. The schema and queries
are deliberately plain SQL so a ClickHouse/Postgres-backed store can implement the same methods.
"""
import json
import sqlite3
import threading
import time
from typing import Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS spans (
    trace_id TEXT, span_id TEXT, parent_span_id TEXT, service TEXT, name TEXT, kind INTEGER,
    start_ns INTEGER, end_ns INTEGER, duration_ms REAL, status_code INTEGER, status_message TEXT,
    peer TEXT, attrs TEXT
);
CREATE INDEX IF NOT EXISTS spans_service_start ON spans (service, start_ns);
CREATE INDEX IF NOT EXISTS spans_start ON spans (start_ns);
CREATE INDEX IF NOT EXISTS spans_trace ON spans (trace_id, span_id);
CREATE TABLE IF NOT EXISTS logs (
    time_ns INTEGER, service TEXT, severity_text TEXT, severity_number INTEGER, body TEXT,
    trace_id TEXT, span_id TEXT, attrs TEXT
);
CREATE INDEX IF NOT EXISTS logs_service_time ON logs (service, time_ns);
CREATE INDEX IF NOT EXISTS logs_time ON logs (time_ns);
CREATE INDEX IF NOT EXISTS logs_trace ON logs (trace_id);
CREATE TABLE IF NOT EXISTS metric_points (
    time_ns INTEGER, start_ns INTEGER, service TEXT, name TEXT, kind TEXT, cumulative INTEGER, attrs TEXT,
    value REAL, count REAL, bounds TEXT, buckets TEXT
);
CREATE INDEX IF NOT EXISTS metric_points_name_time ON metric_points (name, time_ns);
CREATE INDEX IF NOT EXISTS metric_points_time ON metric_points (time_ns);
"""

SPAN_COLS = ("trace_id", "span_id", "parent_span_id", "service", "name", "kind", "start_ns", "end_ns",
             "duration_ms", "status_code", "status_message", "peer", "attrs")
LOG_COLS = ("time_ns", "service", "severity_text", "severity_number", "body", "trace_id", "span_id", "attrs")
# OTel metric data points: kind gauge | sum | histogram; value = gauge/sum value or histogram sum
METRIC_COLS = ("time_ns", "start_ns", "service", "name", "kind", "cumulative", "attrs", "value", "count", "bounds", "buckets")
_JSON_COLS = ("attrs", "bounds", "buckets")

# OTLP enums
KIND_SERVER, KIND_CLIENT, KIND_PRODUCER, KIND_CONSUMER = 2, 3, 4, 5
STATUS_ERROR = 2


class Store:
    def __init__(self, path: str):
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.executescript(SCHEMA)

    def _rows(self, sql: str, args: Iterable = ()) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self._db.execute(sql, tuple(args)).fetchall()]

    def insert_spans(self, spans: list[dict]) -> None:
        if not spans:
            return
        with self._lock, self._db:
            self._db.executemany(
                f"INSERT INTO spans ({','.join(SPAN_COLS)}) VALUES ({','.join('?' * len(SPAN_COLS))})",
                [tuple(json.dumps(s.get(c) or {}) if c == "attrs" else s.get(c) for c in SPAN_COLS) for s in spans])

    def insert_logs(self, logs: list[dict]) -> None:
        if not logs:
            return
        with self._lock, self._db:
            self._db.executemany(
                f"INSERT INTO logs ({','.join(LOG_COLS)}) VALUES ({','.join('?' * len(LOG_COLS))})",
                [tuple(json.dumps(r.get(c) or {}) if c == "attrs" else r.get(c) for c in LOG_COLS) for r in logs])

    def insert_metric_points(self, points: list[dict]) -> None:
        if not points:
            return
        with self._lock, self._db:
            self._db.executemany(
                f"INSERT INTO metric_points ({','.join(METRIC_COLS)}) VALUES ({','.join('?' * len(METRIC_COLS))})",
                [tuple(json.dumps(p.get(c)) if c in _JSON_COLS else p.get(c) for c in METRIC_COLS) for p in points])

    def metric_points(self, name: str, start_ns: int, end_ns: int, limit: int = 20000) -> list[dict]:
        rows = self._rows(f"SELECT * FROM metric_points WHERE name = ? AND time_ns >= ? AND time_ns <= ? "
                          f"ORDER BY time_ns LIMIT {int(limit)}", [name, start_ns, end_ns])
        for r in rows:
            for c in _JSON_COLS:
                r[c] = json.loads(r[c]) if r[c] else None
        return rows

    def metric_attr_samples(self, name: str, since_ns: int, limit: int = 300) -> list[dict]:
        rows = self._rows(f"SELECT attrs FROM metric_points WHERE name = ? AND time_ns >= ? ORDER BY time_ns DESC "
                          f"LIMIT {int(limit)}", [name, since_ns])
        return [json.loads(r["attrs"]) if r["attrs"] else {} for r in rows]

    def metric_names(self, since_ns: int) -> list[dict]:
        return self._rows("SELECT name, kind, service, count(*) AS points FROM metric_points WHERE time_ns >= ? "
                          "GROUP BY 1, 2, 3 ORDER BY 1 LIMIT 1000", [since_ns])

    def prune(self, older_than_ns: int, metrics_older_than_ns: Optional[int] = None) -> None:
        with self._lock, self._db:
            self._db.execute("DELETE FROM spans WHERE start_ns < ?", (older_than_ns,))
            self._db.execute("DELETE FROM logs WHERE time_ns < ?", (older_than_ns,))
            self._db.execute("DELETE FROM metric_points WHERE time_ns < ?",
                             (older_than_ns if metrics_older_than_ns is None else metrics_older_than_ns,))

    # ---------- queries used by incident evidence / traces / topology ----------
    @staticmethod
    def _svc_filter(services: Optional[list[str]], cols: tuple[str, ...]) -> tuple[str, list]:
        if not services:
            return "", []
        marks = ",".join("?" * len(services))
        return " AND (" + " OR ".join(f"{c} IN ({marks})" for c in cols) + ")", list(services) * len(cols)

    def span_durations(self, start_ns: int, end_ns: int, services: Optional[list[str]],
                       limit: int = 50000) -> list[dict]:
        where, args = self._svc_filter(services, ("service",))
        return self._rows(
            f"SELECT service, duration_ms, status_code FROM spans WHERE start_ns >= ? AND start_ns < ?{where} "
            f"LIMIT {int(limit)}", [start_ns, end_ns, *args])

    def error_spans(self, start_ns: int, end_ns: int, services: Optional[list[str]], limit: int = 2000) -> list[dict]:
        where, args = self._svc_filter(services, ("service", "peer"))
        return self._rows(
            f"SELECT * FROM spans WHERE status_code = {STATUS_ERROR} AND start_ns >= ? AND start_ns < ?{where} "
            f"ORDER BY start_ns DESC LIMIT {int(limit)}", [start_ns, end_ns, *args])

    def problem_logs(self, start_ns: int, end_ns: int, services: Optional[list[str]],
                     trace_ids: list[str], limit: int = 2000) -> list[dict]:
        where, args = self._svc_filter(services, ("service",))
        trace_sql, trace_args = "", []
        if trace_ids:
            trace_ids = trace_ids[:500]
            trace_sql = f" OR trace_id IN ({','.join('?' * len(trace_ids))})"
            trace_args = trace_ids
        return self._rows(
            "SELECT * FROM logs WHERE time_ns >= ? AND time_ns < ? AND ("
            f"((severity_number >= 13 OR upper(severity_text) IN ('WARN','WARNING','ERROR','FATAL','CRITICAL')){where})"
            f"{trace_sql}) ORDER BY time_ns DESC LIMIT {int(limit)}",
            [start_ns, end_ns, *args, *trace_args])

    # ---------- Explore: searches a user runs from the UI (parameterised, capped) ----------
    @staticmethod
    def _like(text: str) -> str:
        return "%" + text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"

    def search_spans(self, start_ns: int, end_ns: int, service: Optional[str] = None, name: Optional[str] = None,
                     errors_only: bool = False, min_ms: Optional[float] = None, sort: str = "slowest",
                     limit: int = 200) -> list[dict]:
        sql, args = "SELECT * FROM spans WHERE start_ns >= ? AND start_ns < ?", [start_ns, end_ns]
        if service:
            sql += " AND service = ?"; args.append(service)
        if name:
            sql += " AND name LIKE ? ESCAPE '\\'"; args.append(self._like(name))
        if errors_only:
            sql += f" AND status_code = {STATUS_ERROR}"
        if min_ms is not None:
            sql += " AND duration_ms >= ?"; args.append(float(min_ms))
        sql += " ORDER BY " + ("duration_ms DESC" if sort == "slowest" else "start_ns DESC") + f" LIMIT {int(limit)}"
        return self._rows(sql, args)

    def search_logs(self, start_ns: int, end_ns: int, service: Optional[str] = None, text: Optional[str] = None,
                    severity: Optional[str] = None, limit: int = 200) -> list[dict]:
        sql, args = "SELECT * FROM logs WHERE time_ns >= ? AND time_ns < ?", [start_ns, end_ns]
        if service:
            sql += " AND service = ?"; args.append(service)
        if text:
            sql += " AND body LIKE ? ESCAPE '\\'"; args.append(self._like(text))
        if severity == "error":
            sql += " AND (severity_number >= 17 OR upper(severity_text) IN ('ERROR','FATAL','CRITICAL'))"
        elif severity == "warn":
            sql += " AND (severity_number >= 13 OR upper(severity_text) IN ('WARN','WARNING','ERROR','FATAL','CRITICAL'))"
        return self._rows(sql + f" ORDER BY time_ns DESC LIMIT {int(limit)}", args)

    def services(self, since_ns: int) -> list[dict]:
        """Services that sent spans or logs since since_ns, with counts, for the Explore filters."""
        return self._rows(
            "SELECT service, sum(spans) AS spans, sum(logs) AS logs FROM ("
            " SELECT service, count(*) AS spans, 0 AS logs FROM spans WHERE start_ns >= ? GROUP BY service"
            " UNION ALL SELECT service, 0, count(*) FROM logs WHERE time_ns >= ? GROUP BY service"
            ") WHERE service IS NOT NULL GROUP BY service ORDER BY service", [since_ns, since_ns])

    def trace(self, trace_id: str) -> tuple[list[dict], list[dict]]:
        spans = self._rows("SELECT * FROM spans WHERE trace_id = ? ORDER BY start_ns LIMIT 1000", [trace_id])
        logs = self._rows("SELECT * FROM logs WHERE trace_id = ? ORDER BY time_ns LIMIT 1000", [trace_id])
        return spans, logs

    def service_edges(self, since_ns: int) -> list[dict]:
        """Cross-service parent/child span pairs."""
        return self._rows(
            "SELECT p.service AS parent, c.service AS child, c.kind AS kind, c.peer AS peer, count(*) AS n, max(c.start_ns) AS last "
            "FROM spans c JOIN spans p ON p.trace_id = c.trace_id AND p.span_id = c.parent_span_id "
            "WHERE c.start_ns >= ? AND p.service != c.service GROUP BY 1, 2, 3, 4", [since_ns])

    def peer_edges(self, since_ns: int) -> list[dict]:
        """Client/producer/consumer spans that name the remote side (db host, topic, peer.service)."""
        return self._rows(
            "SELECT service, peer, kind, count(*) AS n, max(start_ns) AS last FROM spans "
            f"WHERE start_ns >= ? AND peer != '' AND kind IN ({KIND_CLIENT},{KIND_PRODUCER},{KIND_CONSUMER}) "
            "GROUP BY 1, 2, 3", [since_ns])


def now_ns() -> int:
    return time.time_ns()
