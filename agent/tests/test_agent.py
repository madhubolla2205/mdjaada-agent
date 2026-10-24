import json
import os
import socket
import sys
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mdjaada_agent import checks, otlp, promtext, tasks  # noqa: E402
from mdjaada_agent.buffer import DiskBuffer  # noqa: E402
from mdjaada_agent.store import Store  # noqa: E402


# ---------- fake services ----------
@pytest.fixture
def fake():
    """Tiny HTTP server: routes[path] = (status, body) ; body may be dict (JSON) or str."""
    routes: dict = {}

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _serve(self):
            self.rfile.read(int(self.headers.get("Content-Length") or 0))  # drain the body, or the client may see a reset
            path = self.path.split("?")[0]
            key = path + ("?state=success" if "state=success" in self.path else "")
            status, body = routes.get(key, routes.get(path, (404, "nope")))
            data = json.dumps(body).encode() if isinstance(body, (dict, list)) else str(body).encode()
            self.send_response(status)
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = _serve

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}", routes
    srv.shutdown()


def iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


# ---------- prometheus ----------
PROM = """
# HELP kafka_consumergroup_lag lag
kafka_consumergroup_lag{consumergroup="orders",topic="orders",partition="0"} 700
kafka_consumergroup_lag{consumergroup="orders",topic="orders",partition="1"} 600
kafka_consumergroup_lag{consumergroup="billing",topic="orders",partition="0"} 5
kafka_topic_partition_under_replicated_partition{topic="orders",partition="0"} 0
weird_metric NaN
"""


def test_promtext_parse_and_select():
    s = promtext.parse(PROM)
    assert len(s) == 4
    name, lbl = promtext.parse_selector('kafka_consumergroup_lag{consumergroup="orders"}')
    assert promtext.aggregate(promtext.select(s, name, lbl), "sum") == 1300


def test_tcp_check():
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen()
    port = srv.getsockname()[1]
    assert checks.check_tcp("127.0.0.1", port).ok
    srv.close()
    assert not checks.check_tcp("127.0.0.1", port).ok


def test_http_checks(fake):
    url, routes = fake
    routes["/health"] = (200, {"status": "UP", "db": {"status": "DOWN"}})
    routes["/broken"] = (503, "unavailable")
    assert checks.run_check({"type": "http", "config": {"health_url": url + "/health"}}).ok
    r = checks.run_check({"type": "http", "config": {"health_url": url + "/health", "json_field": "db.status",
                                                     "json_equals": "UP"}})
    assert not r.ok and "db.status=DOWN" in r.message
    r = checks.run_check({"type": "http", "config": {"health_url": url + "/broken"}})
    assert not r.ok and "503" in r.message
    r = checks.run_check({"type": "http", "config": {"health_url": url + "/health", "latency_ms_warn": -1}})
    assert r.ok and r.warn


def test_env_secret_resolution(fake, monkeypatch):
    url, routes = fake
    routes["/h"] = (200, "ok")
    monkeypatch.setenv("SVC_URL", url + "/h")
    assert checks.run_check({"type": "http", "config": {"health_url": "env:SVC_URL"}}).ok


def test_kafka_lag_and_brokers(fake):
    url, routes = fake
    routes["/metrics"] = (200, PROM)
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen()
    live = f"127.0.0.1:{srv.getsockname()[1]}"
    cfg = {"bootstrap": f"{live},127.0.0.1:1", "metrics_url": url + "/metrics", "consumer_group": "orders",
           "lag_warn": 1000, "lag_fail": 5000}
    r = checks.run_check({"type": "kafka", "config": cfg})
    assert r.ok and r.warn
    assert r.metrics["consumer_lag"] == 1300 and r.metrics["brokers_down"] == 1
    r = checks.run_check({"type": "kafka", "config": {**cfg, "bootstrap": live, "lag_fail": 1000}})
    assert not r.ok and "consumer_lag 1300 > 1000" in r.message
    srv.close()


def test_airflow_failed_run_and_sla(fake):
    url, routes = fake
    now = datetime.now(timezone.utc)
    path = "/api/v1/dags/etl/dagRuns"
    routes[path] = (200, {"dag_runs": [{"dag_run_id": "r2", "state": "failed", "start_date": iso(now)}]})
    routes[path + "?state=success"] = (200, {"dag_runs": [{"end_date": iso(now - timedelta(hours=3))}]})
    r = checks.run_check({"type": "airflow", "config": {"base_url": url, "dag_id": "etl", "sla_s": 3600}})
    assert not r.ok and "failed" in r.message and "SLA miss" in r.message
    routes[path] = (200, {"dag_runs": [{"dag_run_id": "r3", "state": "running",
                                        "start_date": iso(now - timedelta(minutes=50))}]})
    r = checks.run_check({"type": "airflow", "config": {"base_url": url, "dag_id": "etl", "duration_s_warn": 1800}})
    assert r.ok and r.warn and "running for" in r.message


def test_mlflow_metric_regression(fake):
    url, routes = fake

    def run(status, acc):
        return {"info": {"run_id": "abcdef123", "status": status},
                "data": {"metrics": [{"key": "accuracy", "value": acc}]}}

    routes["/api/2.0/mlflow/runs/search"] = (200, {"runs": [run("FINISHED", 0.80), run("FINISHED", 0.92)]})
    r = checks.run_check({"type": "mlflow", "config": {"tracking_url": url, "experiment_id": "1",
                                                       "metric": "accuracy", "regression_pct": 5}})
    assert r.ok and r.warn and "accuracy 0.92 -> 0.8" in r.message
    routes["/api/2.0/mlflow/runs/search"] = (200, {"runs": [run("FAILED", 0), run("FINISHED", 0.92)]})
    assert not checks.run_check({"type": "mlflow", "config": {"tracking_url": url, "experiment_id": "1"}}).ok


def test_unconfigured_node_is_skipped():
    assert checks.run_check({"type": "kafka", "config": {}}) is None


# ---------- disk buffer ----------
def test_buffer_on_a_full_disk_drops_instead_of_crashing(tmp_path, monkeypatch):
    buf = DiskBuffer(str(tmp_path / "b.jsonl"))
    assert buf.append({"i": 1})
    import builtins
    real_open = builtins.open

    def no_space(path, mode="r", *a, **kw):
        if "a" in mode or "w" in mode:
            raise OSError(28, "No space left on device")
        return real_open(path, mode, *a, **kw)
    monkeypatch.setattr(builtins, "open", no_space)
    assert buf.append({"i": 2}) is False and buf.dropped == 1   # no exception: the agent keeps checking
    assert buf.drain(lambda item: None) == 1                       # what was buffered before is still sent



def test_buffer_order_and_cap(tmp_path):
    buf = DiskBuffer(str(tmp_path / "b.jsonl"), max_bytes=2000)
    for i in range(100):
        buf.append({"i": i, "pad": "x" * 30})
    assert 0 < len(buf) < 100  # oldest dropped
    sent = []

    def flaky(item):
        if len(sent) == 3:
            raise OSError("api down")
        sent.append(item["i"])

    remaining = len(buf)
    assert buf.drain(flaky) == 3
    assert sent == sorted(sent) and sent[-1] < 99
    assert len(buf) == remaining - 3
    buf.drain(lambda item: sent.append(item["i"]))
    assert len(buf) == 0 and sent[-1] == 99


# ---------- OTLP -> store -> evidence ----------
def _attr(k, v):
    return {"key": k, "value": {"stringValue": v}}


def otlp_spans(now_ns, trace_id="0af7651916cd43dd8448eb211c80319c"):
    def span(span_id, parent, name, kind, dur_ms, error=False, peer=None, msg="", t0=0):
        s = {"traceId": trace_id, "spanId": span_id, "parentSpanId": parent, "name": name, "kind": kind,
             "startTimeUnixNano": str(now_ns + t0), "endTimeUnixNano": str(now_ns + t0 + int(dur_ms * 1e6)),
             "status": {"code": 2 if error else 0, "message": msg}, "attributes": []}
        if peer:
            s["attributes"].append(_attr("server.address", peer))
        return s

    def rs(service, spans):
        return {"resource": {"attributes": [_attr("service.name", service)]}, "scopeSpans": [{"spans": spans}]}

    return {"resourceSpans": [
        rs("web", [span("a1", "", "GET /checkout", "SPAN_KIND_SERVER", 900),
                   span("a2", "a1", "POST /orders", "SPAN_KIND_CLIENT", 850)]),
        rs("orders-api", [span("b1", "a2", "POST /orders", 2, 840, error=True, msg="db timeout after 800ms"),
                          span("b2", "b1", "INSERT orders", 3, 800, error=True, peer="db.internal",
                               msg="connection to 10.0.0.5 timed out")]),
    ]}


def otlp_logs(now_ns, trace_id="0af7651916cd43dd8448eb211c80319c"):
    return {"resourceLogs": [{"resource": {"attributes": [_attr("service.name", "orders-api")]}, "scopeLogs": [
        {"logRecords": [
            {"timeUnixNano": str(now_ns), "severityText": "ERROR", "body": {"stringValue": "insert failed id=42"},
             "traceId": trace_id, "spanId": "b2"},
            {"timeUnixNano": str(now_ns), "severityNumber": "SEVERITY_NUMBER_INFO",
             "body": {"stringValue": "retrying"}, "traceId": trace_id},
            {"timeUnixNano": str(now_ns), "severityText": "INFO", "body": {"stringValue": "unrelated"}},
        ]}]}]}


@pytest.fixture
def receiver(tmp_path):
    store = Store(str(tmp_path / "t.db"))
    srv = otlp.serve(store, 0, host="127.0.0.1", sample_rate=1.0)
    yield store, f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


def post(url, doc, ctype="application/json"):
    req = urllib.request.Request(url, data=json.dumps(doc).encode(), headers={"Content-Type": ctype})
    return urllib.request.urlopen(req, timeout=5).status


def test_otlp_full_disk_is_a_retryable_error_not_a_bad_payload(receiver, monkeypatch):
    store, url = receiver
    import sqlite3

    def full(*a):
        raise sqlite3.OperationalError("database or disk is full")
    monkeypatch.setattr(store, "insert_spans", full)
    with pytest.raises(urllib.error.HTTPError) as e:
        post(url + "/v1/traces", otlp_spans(time.time_ns()))
    assert e.value.code == 503 and b"storage unavailable" in e.value.read()


def test_otlp_ingest_evidence_trace_and_topology(receiver):
    store, url = receiver
    now = time.time_ns()
    assert post(url + "/v1/traces", otlp_spans(now)) == 200
    assert post(url + "/v1/logs", otlp_logs(now)) == 200
    with pytest.raises(urllib.error.HTTPError) as e:
        post(url + "/v1/traces", {}, "application/x-protobuf")
    assert e.value.code == 415

    start = iso(datetime.now(timezone.utc) - timedelta(minutes=5))
    ev = tasks.evidence(store, {"services": ["orders-api", "db.internal", "web"], "start": start, "end": None})
    assert ev["services"]["orders-api"]["error_spans"] == 2
    assert ev["services"]["db.internal"]["error_spans"] == 1  # credited via the client span's peer
    assert ev["services"]["orders-api"]["error_logs"] == 1
    msgs = {g["message"] for g in ev["error_groups"]}
    assert {"connection to <ip> timed out", "db timeout after <n>ms"} <= msgs
    # trace-to-log linking: the INFO "retrying" line is included because it shares the error trace id
    bodies = [r["body"] for r in ev["logs"]]
    assert "retrying" in bodies and "unrelated" not in bodies
    assert [g["message"] for g in ev["log_groups"]] == ["insert failed id=<n>"]

    tr = tasks.trace(store, {"trace_id": "0AF7651916CD43DD8448EB211C80319C"})
    assert [s["span_id"] for s in tr["spans"]] == ["a1", "a2", "b1", "b2"] and len(tr["logs"]) == 2

    pairs = {(p["source"], p["target"], p["relation"]) for p in tasks.topology(store, now - 10**9)}
    assert ("orders-api", "web", "serves") in pairs
    assert ("db.internal", "orders-api", "serves") in pairs


def test_sampling_keeps_errors_and_slow_spans():
    assert otlp.keep_span("ffffffffffffffff", True, 1, 500, 0.0)
    assert otlp.keep_span("ffffffffffffffff", False, 600, 500, 0.0)
    assert not otlp.keep_span("ffffffffffffffff", False, 5, 500, 0.0)
    assert otlp.keep_span("0000000000000000", False, 5, 500, 0.1)


def test_test_node_task(fake):
    url, routes = fake
    routes["/h"] = (500, "boom")
    out = tasks.test_node({"type": "http", "config": {"health_url": url + "/h"}})
    assert out["ok"] is False and "500" in out["message"]
    assert "nothing to check" in tasks.test_node({"type": "kafka", "config": {}})["message"]


def test_nodes_assigned_to_another_agent_are_skipped(monkeypatch, tmp_path):
    from mdjaada_agent.main import Agent
    monkeypatch.setenv("MDJAADA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("MDJAADA_AGENT_ID", "host-1")
    a = Agent()
    shared, mine, theirs = ({"config": {}}, {"config": {"agent": "host-1"}}, {"config": {"agent": "k8s-agent"}})
    assert a.mine(shared) and a.mine(mine) and not a.mine(theirs)
    a.assigned_only = True
    assert not a.mine(shared) and a.mine(mine)


def test_old_flowtrace_settings_still_work(monkeypatch, tmp_path):
    from mdjaada_agent.main import Agent
    monkeypatch.setenv("FLOWTRACE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("FLOWTRACE_AGENT_ID", "legacy-agent")
    monkeypatch.delenv("MDJAADA_AGENT_ID", raising=False)
    assert Agent().agent_id == "legacy-agent"
    monkeypatch.setenv("MDJAADA_AGENT_ID", "new-agent")
    assert Agent().agent_id == "new-agent"  # the new name wins


def test_airflow3_token_login_and_refresh(fake):
    """Airflow 3: /api/v2 with a JWT from POST /auth/token; basic auth is rejected; expired tokens are renewed."""
    url, routes = fake
    issued = []
    now = datetime.now(timezone.utc)
    routes["/api/v2/version"] = (200, {"version": "3.1.0"})
    routes["/api/v2/dags/etl/dagRuns"] = (200, {"dag_runs": [{"dag_run_id": "r1", "state": "success",
                                                               "start_date": iso(now)}]})
    real = checks.http_get

    def http_get(u, cfg=None, data=None, method=None):
        if u.endswith("/auth/token"):
            assert json.loads(data) == {"username": "admin", "password": "pw"}
            issued.append(f"tok{len(issued)}")
            return 201, json.dumps({"access_token": issued[-1]}).encode(), 1.0
        if "/api/v2/dags/" in u and (cfg or {}).get("token") != issued[-1]:
            raise checks.urllib.error.HTTPError(u, 401, "Unauthorized", {}, None)
        return real(u, cfg, data, method)

    checks._airflow_cache.clear()
    try:
        checks.http_get = http_get
        cfg = {"base_url": url, "dag_id": "etl", "username": "admin", "password": "pw"}
        r = checks.run_check({"type": "airflow", "config": cfg})
        assert r.ok and r.metrics["airflow_api"] == "v2" and issued == ["tok0"]
        issued.append("rotated-by-server")  # cached token no longer accepted -> one retry with a fresh token
        r = checks.run_check({"type": "airflow", "config": cfg})
        assert r.ok and issued[-1] == "tok2"
    finally:
        checks.http_get = real
        checks._airflow_cache.clear()



def test_a_hung_check_does_not_stall_the_other_nodes(monkeypatch, tmp_path):
    import threading as th
    from mdjaada_agent import main as agent_main
    monkeypatch.setenv("MDJAADA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("MDJAADA_CHECK_DEADLINE", "0.5")
    a = agent_main.Agent()
    release = th.Event()
    posted = []
    a.api.post_or_buffer = lambda path, body: posted.append(body)

    def fake_run(node):
        if node["node_id"] == "stuck":
            release.wait(5)  # a driver call that never returns in time
        return checks.Result(True, "fine")
    monkeypatch.setattr(agent_main.checks, "run_check", fake_run)
    a.nodes = [{"node_id": "stuck", "type": "http", "config": {}}, {"node_id": "ok", "type": "http", "config": {}}]
    a.check_cycle(force=True)
    by_node = {r["node_id"]: r for r in posted[0]["results"]}
    assert by_node["ok"]["ok"] and not by_node["stuck"]["ok"] and "did not finish" in by_node["stuck"]["message"]
    a.check_cycle(force=True)  # still hung: not started a second time
    assert [r["node_id"] for r in posted[1]["results"]] == ["ok"]
    release.set()


def test_every_request_identifies_the_agent(monkeypatch):
    """Python's default 'Python-urllib/x.y' is blocked by Cloudflare's bot protection (403), so the agent
    must send its own User-Agent to the platform and in HTTP checks."""
    import urllib.request
    from mdjaada_agent import USER_AGENT, checks, client
    from mdjaada_agent.buffer import DiskBuffer
    seen = []

    class Resp:
        status = 200
        headers = {}
        def read(self): return b'{"status": "UP"}'
        def getcode(self): return 200
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake(req, *a, **kw):
        seen.append(req.get_header("User-agent"))
        return Resp()
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    import tempfile
    client.Api("https://app.mdjaada.com/api", "k", DiskBuffer(tempfile.mkdtemp())).call("GET", "/auth/config")
    try:
        checks.http_get("https://example.test/health")
    except Exception:
        pass
    assert seen and all(ua == USER_AGENT for ua in seen), seen
    assert USER_AGENT.startswith("mdjaada-agent/")


def test_postgres_queries_stuck_on_locks():
    from mdjaada_agent.checks import pg_lock_waits
    assert pg_lock_waits({}, lambda q: "0,0") == []                      # nothing waiting: no extra line
    r = pg_lock_waits({}, lambda q: "4,25.4")[0]
    assert r.ok and r.warn and "4 queries waiting on locks, longest 25.4 s > 10 s" in r.message
    assert r.metrics == {"lock_waiting_queries": 4.0, "longest_lock_wait_s": 25.4}
    assert not pg_lock_waits({}, lambda q: "4,75")[0].ok                  # past 60 s: the app's queries are stuck
    assert pg_lock_waits({"lock_wait_s_fail": "300"}, lambda q: "4,75")[0].ok
    assert pg_lock_waits({}, lambda q: (_ for _ in ()).throw(RuntimeError("permission denied"))) == []
