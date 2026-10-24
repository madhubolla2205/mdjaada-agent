"""OTLP metrics: receiver, local store, and rules that judge a node on a metric its apps send."""
import json
import time
import urllib.request

import pytest

from mdjaada_agent import checks, otelrules, otlp, tasks
from mdjaada_agent.store import Store

S = 1_000_000_000


def kv(d):
    return [{"key": k, "value": {"intValue": str(v)} if isinstance(v, int) else {"stringValue": v}}
            for k, v in d.items()]


def doc(service, metrics):
    return {"resourceMetrics": [{"resource": {"attributes": kv({"service.name": service})},
                                 "scopeMetrics": [{"metrics": metrics}]}]}


def counter(name, points, cumulative=True):
    return {"name": name, "sum": {"aggregationTemporality": 2 if cumulative else 1, "isMonotonic": True,
                                  "dataPoints": [{"attributes": kv(a), "timeUnixNano": str(t), "asInt": str(v)}
                                                 for t, v, a in points]}}


def gauge(name, points):
    return {"name": name, "gauge": {"dataPoints": [{"attributes": kv(a), "timeUnixNano": str(t), "asDouble": v}
                                                   for t, v, a in points]}}


def histogram(name, points, bounds=(0.1, 0.5, 1.0)):
    return {"name": name, "histogram": {"aggregationTemporality": "AGGREGATION_TEMPORALITY_CUMULATIVE", "dataPoints": [
        {"attributes": kv(a), "timeUnixNano": str(t), "count": str(sum(b)), "sum": s,
         "explicitBounds": list(bounds), "bucketCounts": [str(x) for x in b]} for t, s, b, a in points]}}


@pytest.fixture
def store(tmp_path):
    st = Store(str(tmp_path / "t.db"))
    otelrules.store = st
    yield st
    otelrules.store = None


def test_parse_kinds_and_resource_attrs():
    now = time.time_ns()
    pts = otlp.parse_metrics(doc("orders", [
        gauge("pool.used", [(now, 7.0, {"pool": "main"})]),
        counter("http.requests", [(now, 42, {"code": 200})]),
        histogram("http.duration", [(now, 3.0, [5, 3, 1, 1], {})]),
    ]))
    by = {p["name"]: p for p in pts}
    assert by["pool.used"]["kind"] == "gauge" and by["pool.used"]["value"] == 7.0
    assert by["pool.used"]["attrs"] == {"service.name": "orders", "pool": "main"}
    assert by["http.requests"]["kind"] == "sum" and by["http.requests"]["cumulative"] == 1
    assert by["http.duration"]["count"] == 10 and by["http.duration"]["buckets"] == [5, 3, 1, 1]


def test_counter_rate_handles_reset_and_filters(store):
    now = time.time_ns()
    store.insert_metric_points(otlp.parse_metrics(doc("orders", [counter("http.requests", [
        (now - 60 * S, 100, {"code": 200}), (now, 160, {"code": 200}),   # +60 in 60 s
        (now - 60 * S, 50, {"code": 500}), (now, 10, {"code": 500}),     # restarted: +10
    ])])))
    r = otelrules.check({"otel_metric": "http.requests", "otel_filter": "service.name=orders"})
    assert r.metrics == {"http.requests rate": pytest.approx(70 / 60, abs=1e-3)}
    r = otelrules.check({"otel_metric": "http.requests", "otel_filter": "http.status=~5..,code=~5.."})
    assert "no OTel data" in r.message and r.warn  # attribute names must match exactly
    r = otelrules.check({"otel_metric": "http.requests", "otel_filter": "code=~5..", "otel_agg": "increase"})
    assert r.metrics == {"http.requests increase": 10}


def test_error_ratio_in_percent_with_limits(store):
    now = time.time_ns()
    store.insert_metric_points(otlp.parse_metrics(doc("orders", [counter("http.requests", [
        (now - 60 * S, 0, {"code": 200}), (now, 90, {"code": 200}),
        (now - 60 * S, 0, {"code": 503}), (now, 10, {"code": 503}),
    ])])))
    cfg = {"otel_metric": "http.requests", "otel_filter": "code=~5..", "otel_ratio_of": "service.name=orders",
           "otel_label": "error rate", "otel_warn": "5", "otel_fail": "20"}
    r = otelrules.check(cfg)
    assert r.metrics == {"error rate": pytest.approx(10.0)} and r.ok and r.warn
    r = otelrules.check({**cfg, "otel_filter": "code=~4.."})
    assert r.metrics == {"error rate": 0.0} and r.ok and not r.warn  # no errors at all is 0 %, not "no data"


def test_histogram_p95_avg_and_rate(store):
    now = time.time_ns()
    # 0-0.1: 80, 0.1-0.5: 15, 0.5-1: 5 observations in the window
    store.insert_metric_points(otlp.parse_metrics(doc("orders", [histogram("http.duration", [
        (now - 60 * S, 10.0, [100, 0, 0, 0], {}), (now, 30.0, [180, 15, 5, 0], {})])])))
    p95 = otelrules.check({"otel_metric": "http.duration", "otel_scale": "1000"})
    assert list(p95.metrics) == ["http.duration p95"]
    assert p95.metrics["http.duration p95"] == pytest.approx(500.0)  # 95th of 100 sits at the top of 0.1-0.5
    avg = otelrules.check({"otel_metric": "http.duration", "otel_agg": "avg"})
    assert avg.metrics["http.duration avg"] == pytest.approx(20.0 / 100)
    rate = otelrules.check({"otel_metric": "http.duration", "otel_agg": "rate"})
    assert rate.metrics["http.duration rate"] == pytest.approx(100 / 60, abs=1e-3)
    fail = otelrules.check({"otel_metric": "http.duration", "otel_scale": "1000", "otel_fail": "300"})
    assert not fail.ok and "> 300" in fail.message


def test_gauge_window_and_missing_modes(store):
    now = time.time_ns()
    store.insert_metric_points(otlp.parse_metrics(doc("orders", [gauge("pool.used", [
        (now - 600 * S, 99.0, {}), (now - 30 * S, 4.0, {}), (now, 8.0, {})])])))
    assert otelrules.check({"otel_metric": "pool.used"}).metrics == {"pool.used avg": 6.0}  # old point is outside
    assert otelrules.check({"otel_metric": "pool.used", "otel_agg": "max"}).metrics == {"pool.used max": 8.0}
    assert otelrules.check({"otel_metric": "pool.used", "otel_window_s": "900", "otel_agg": "max"}).metrics == {
        "pool.used max": 99.0}
    assert not otelrules.check({"otel_metric": "nope", "otel_missing": "fail"}).ok
    ok = otelrules.check({"otel_metric": "nope", "otel_missing": "ok"})
    assert ok.ok and not ok.warn
    assert not otelrules.check({"otel_metric": "pool.used", "otel_filter": "garbage"}).ok
    assert not otelrules.check({"otel_metric": "pool.used", "otel_agg": "median"}).ok


def test_rule_runs_with_the_node_checks(store):
    now = time.time_ns()
    store.insert_metric_points(otlp.parse_metrics(doc("orders", [gauge("queue.depth", [(now, 12.0, {})])])))
    r = checks.run_check({"node_id": "n1", "type": "custom", "config": {
        "otel_metric": "queue.depth", "otel_label": "depth", "otel_warn": "10"}})
    assert r.ok and r.warn and r.metrics == {"depth": 12.0}
    names = tasks.run(store, "metric_names", {})["metrics"]
    assert names == [{"name": "queue.depth", "kind": "gauge", "service": "orders", "points": 1}]


def test_receiver_accepts_metrics_over_http(store):
    server = otlp.serve(store, 0, host="127.0.0.1")
    try:
        body = json.dumps(doc("web", [gauge("cpu", [(time.time_ns(), 0.5, {})])])).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{server.server_port}/v1/metrics", data=body,
                                     headers={"Content-Type": "application/json"})
        assert urllib.request.urlopen(req, timeout=5).status == 200
        assert otelrules.check({"otel_metric": "cpu"}).metrics == {"cpu avg": 0.5}
    finally:
        server.shutdown()


def test_prune_keeps_metrics_on_their_own_retention(store):
    now = time.time_ns()
    store.insert_metric_points(otlp.parse_metrics(doc("w", [gauge("cpu", [(now - 7200 * S, 1.0, {}), (now, 2.0, {})])])))
    store.prune(now - 86400 * S, now - 3600 * S)
    assert [p["value"] for p in store.metric_points("cpu", 0, now)] == [2.0]


def test_series_that_starts_inside_the_window_counts_fully(store):
    """The first 5xx after a healthy period: one export only, but its start time is inside the window."""
    now = time.time_ns()
    ok = {"name": "http.requests", "sum": {"aggregationTemporality": 2, "isMonotonic": True, "dataPoints": [
        {"attributes": kv({"code": 200}), "startTimeUnixNano": str(now - 3600 * S), "timeUnixNano": str(t), "asInt": str(v)}
        for t, v in ((now - 200 * S, 1000), (now - 100 * S, 1500), (now, 1900))]}}
    bad = {"name": "http.requests", "sum": {"aggregationTemporality": 2, "isMonotonic": True, "dataPoints": [
        {"attributes": kv({"code": 503}), "startTimeUnixNano": str(now - 50 * S), "timeUnixNano": str(now), "asInt": "100"}]}}
    store.insert_metric_points(otlp.parse_metrics(doc("orders", [ok, bad])))
    cfg = {"otel_metric": "http.requests", "otel_filter": "code=503", "otel_ratio_of": "service.name=orders",
           "otel_window_s": "150"}
    # window = last 150 s: 200s: baseline 1000 (export before the window) -> 1900 = 900; 503s: all 100 since start
    assert otelrules.check(cfg).metrics == {"http.requests %": pytest.approx(100 * 100 / 1000)}
    r = otelrules.check({"otel_metric": "http.requests", "otel_filter": "code=503", "otel_window_s": "150"})
    assert r.metrics == {"http.requests rate": pytest.approx(2.0)}  # 100 over the 50 s since it started


def test_up_down_counter_is_a_level_not_a_rate(store):
    now = time.time_ns()
    usage = {"name": "db.client.connections.usage", "sum": {"aggregationTemporality": 2, "isMonotonic": False,
             "dataPoints": [{"timeUnixNano": str(t), "asInt": str(v)} for t, v in ((now - 20 * S, 4), (now, 9))]}}
    store.insert_metric_points(otlp.parse_metrics(doc("orders", [usage])))
    assert otelrules.check({"otel_metric": "db.client.connections.usage"}).metrics == {
        "db.client.connections.usage avg": 6.5}
    assert otelrules.check({"otel_metric": "db.client.connections.usage", "otel_agg": "last"}).metrics == {
        "db.client.connections.usage last": 9}


def test_nothing_new_in_the_window_is_no_data(store):
    now = time.time_ns()
    store.insert_metric_points(otlp.parse_metrics(doc("orders", [gauge("cpu", [(now - 300 * S, 1.0, {})])])))
    assert "no OTel data" in otelrules.check({"otel_metric": "cpu", "otel_window_s": "180"}).message
