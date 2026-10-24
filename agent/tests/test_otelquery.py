"""Explore charts for any OTel metric: catalog and series computed from real OTLP JSON in the agent's store."""
import time

from mdjaada_agent import otlp, tasks
from mdjaada_agent.store import Store

SEC = 10**9


def rm(service, metrics):
    return {"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
            "scopeMetrics": [{"metrics": metrics}]}


def attr(**kw):
    return [{"key": k, "value": {"stringValue": v}} for k, v in kw.items()]


def store_with_metrics(tmp_path):
    s = Store(str(tmp_path / "t.db"))
    start = time.time_ns() - 600 * SEC   # relative to when the test runs, not when the file was loaded
    docs = []
    for i in range(60):   # every 10 s for the last 10 minutes
        t = str(start + i * 10 * SEC)
        st = str(start - 10 * SEC)
        mem = lambda base: {"name": "process.memory.usage", "gauge": {"dataPoints": [  # noqa: E731
            {"timeUnixNano": t, "asDouble": base + (i % 2) * 10}]}}
        # cumulative counter: +10 per 10 s for /orders, +20 for /pay; the /pay counter restarts at i == 30
        pay = 20 * (i - 30) + 10 if i >= 30 else 20 * i   # restarted 5 s before i == 30: 10 counted since
        req = {"name": "http.server.requests", "sum": {"aggregationTemporality": 2, "isMonotonic": True, "dataPoints": [
            {"timeUnixNano": t, "startTimeUnixNano": st, "asInt": str(10 * i), "attributes": attr(**{"http.route": "/orders"})},
            {"timeUnixNano": t, "startTimeUnixNano": st if i < 30 else str(start + 295 * SEC), "asInt": str(pay),
             "attributes": attr(**{"http.route": "/pay"})}]}}
        # cumulative histogram: each 10 s, 10 requests: 9 under 0.1 s, 1 between 0.5 and 1 s
        dur = {"name": "http.server.request.duration", "histogram": {"aggregationTemporality": 2, "dataPoints": [
            {"timeUnixNano": t, "startTimeUnixNano": st, "count": str(10 * (i + 1)), "sum": 1.5 * (i + 1),
             "explicitBounds": [0.1, 0.5, 1.0], "bucketCounts": [str(9 * (i + 1)), "0", str(i + 1), "0"],
             "attributes": attr(**{"http.route": "/orders"})}]}}
        docs.append({"resourceMetrics": [rm("orders-api", [mem(100), req, dur]), rm("payments-api", [mem(300)])]})
    for d in docs:
        s.insert_metric_points(otlp.parse_metrics(d))
    return s


def lines(s, **q):
    r = tasks.run(s, "search", {"what": "metric_series", "minutes": 10, **q})
    return r, {x["label"]: [v for _, v in x["points"] if v is not None] for x in r["series"]}


def test_catalog_lists_every_metric_with_kind_and_split_keys(tmp_path):
    s = store_with_metrics(tmp_path)
    cat = {m["name"]: m for m in tasks.run(s, "search", {"what": "metric_catalog", "minutes": 60})["metrics"]}
    assert set(cat) == {"process.memory.usage", "http.server.requests", "http.server.request.duration"}
    assert cat["process.memory.usage"]["kind"] == "gauge" and sorted(cat["process.memory.usage"]["services"]) == ["orders-api", "payments-api"]
    assert cat["http.server.requests"]["default_agg"] == "rate" and cat["http.server.request.duration"]["default_agg"] == "p95"
    keys = [a["key"] for a in cat["http.server.requests"]["attributes"]]
    assert keys[0] == "http.route"   # two values: a good "split by" key comes first


def test_gauge_lines_split_by_service(tmp_path):
    s = store_with_metrics(tmp_path)
    r, ls = lines(s, metric="process.memory.usage", group_by="service.name")
    assert r["kind"] == "gauge" and r["agg"] == "avg" and set(ls) == {"orders-api", "payments-api"}
    assert all(100 <= v <= 110 for v in ls["orders-api"]) and all(300 <= v <= 310 for v in ls["payments-api"])
    assert len(r["series"][0]["points"]) <= 120


def test_counter_rate_per_route_survives_a_restart(tmp_path):
    s = store_with_metrics(tmp_path)
    r, ls = lines(s, metric="http.server.requests", group_by="http.route")
    assert r["agg"] == "rate"
    assert all(abs(v - 1.0) < 0.05 for v in ls["/orders"][1:])          # +10 per 10 s
    assert all(abs(v - 2.0) < 0.05 for v in ls["/pay"][1:])             # +20 per 10 s, also after the reset
    _, total = lines(s, metric="http.server.requests")                  # no split: one line, rates add up
    assert all(abs(v - 3.0) < 0.1 for v in total["all"][1:])
    _, only = lines(s, metric="http.server.requests", filter="http.route=/pay")
    assert all(abs(v - 2.0) < 0.05 for v in only["all"][1:])


def test_histogram_percentiles_and_average(tmp_path):
    s = store_with_metrics(tmp_path)
    r, p95 = lines(s, metric="http.server.request.duration")
    assert r["agg"] == "p95" and all(0.5 < v <= 1.0 for v in p95["all"][1:])   # the slowest 10% land in 0.5-1 s
    _, p50 = lines(s, metric="http.server.request.duration", agg="p50")
    assert all(v <= 0.1 for v in p50["all"][1:])
    _, avg = lines(s, metric="http.server.request.duration", agg="avg")
    assert all(abs(v - 0.15) < 0.01 for v in avg["all"][1:])
    _, wrong = lines(s, metric="http.server.request.duration", agg="increase-everything")   # unknown: default
    assert wrong


def test_unknown_metric_and_too_many_lines(tmp_path):
    s = store_with_metrics(tmp_path)
    assert tasks.run(s, "search", {"what": "metric_series", "metric": "nope"})["series"] == []
    now = time.time_ns()
    pts = [{"time_ns": now - i * SEC, "start_ns": 0, "service": "x", "name": "many", "kind": "gauge", "cumulative": 0,
            "attrs": {"id": str(i)}, "value": float(i), "count": None, "bounds": None, "buckets": None} for i in range(40)]
    s.insert_metric_points(pts)
    r = tasks.run(s, "search", {"what": "metric_series", "metric": "many", "group_by": "id", "minutes": 5})
    assert r["truncated"] and len(r["series"]) == 12 and r["series"][0]["label"] == "39"   # busiest lines first
