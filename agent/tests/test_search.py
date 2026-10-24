"""Explore searches the agent runs on its local store (spans, logs, services)."""
import time

from mdjaada_agent import tasks
from mdjaada_agent.store import Store


def _store(tmp_path):
    s = Store(str(tmp_path / "t.db"))
    now = time.time_ns()
    span = lambda sid, svc, name, ago_s, ms, code: {  # noqa: E731
        "trace_id": "t" + sid, "span_id": sid, "parent_span_id": None, "service": svc, "name": name, "kind": 2,
        "start_ns": now - ago_s * 10**9, "end_ns": now, "duration_ms": ms, "status_code": code,
        "status_message": "", "peer": None, "attrs": {}}
    s.insert_spans([span("a", "orders-api", "POST /orders", 1, 950.0, 2),
                    span("b", "orders-api", "GET /orders/100%_x", 2, 12.0, 1),
                    span("c", "payments", "charge", 3, 300.0, 1),
                    span("d", "payments", "old", 100 * 3600, 5.0, 1)])  # older than the 72 h store window
    log = lambda svc, sev, num, body, ago_s: {"time_ns": now - ago_s * 10**9, "service": svc, "severity_text": sev,  # noqa: E731
                                              "severity_number": num, "body": body, "trace_id": None, "span_id": None, "attrs": {}}
    s.insert_logs([log("orders-api", "ERROR", 17, "connection refused to postgres", 1),
                   log("orders-api", "WARN", 13, "slow query", 2),
                   log("orders-api", "INFO", 9, "order 100% created", 3)])
    return s


def names(r):
    return [x["name"] for x in r["spans"]]


def test_span_filters(tmp_path):
    s = _store(tmp_path)
    assert names(tasks.run(s, "search", {"what": "spans", "minutes": 5})) == ["POST /orders", "charge", "GET /orders/100%_x"]
    assert names(tasks.run(s, "search", {"what": "spans", "sort": "newest"}))[0] == "POST /orders"
    assert names(tasks.run(s, "search", {"what": "spans", "errors_only": True})) == ["POST /orders"]
    assert names(tasks.run(s, "search", {"what": "spans", "min_ms": 100, "service": "payments"})) == ["charge"]
    assert names(tasks.run(s, "search", {"what": "spans", "minutes": 10**6})) == names(
        tasks.run(s, "search", {"what": "spans", "minutes": 72 * 60}))  # capped to the 72 h the store keeps
    assert len(tasks.run(s, "search", {"what": "spans", "limit": 10**6})["spans"]) <= tasks.MAX_SEARCH_ROWS


def test_like_wildcards_are_literal_and_values_are_parameters(tmp_path):
    s = _store(tmp_path)
    assert names(tasks.run(s, "search", {"what": "spans", "name": "%"})) == ["GET /orders/100%_x"]
    assert names(tasks.run(s, "search", {"what": "spans", "name": "100%_"})) == ["GET /orders/100%_x"]
    assert tasks.run(s, "search", {"what": "logs", "service": "x' OR '1'='1"})["logs"] == []


def test_log_filters_and_services(tmp_path):
    s = _store(tmp_path)
    assert [x["body"] for x in tasks.run(s, "search", {"what": "logs", "text": "refused"})["logs"]] == ["connection refused to postgres"]
    assert [x["severity"] for x in tasks.run(s, "search", {"what": "logs", "severity": "error"})["logs"]] == ["ERROR"]
    assert [x["severity"] for x in tasks.run(s, "search", {"what": "logs", "severity": "warn"})["logs"]] == ["ERROR", "WARN"]
    svcs = {x["service"]: x for x in tasks.run(s, "search", {"what": "services"})["services"]}
    assert svcs["orders-api"]["spans"] == 2 and svcs["orders-api"]["logs"] == 3
