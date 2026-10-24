import copy
import json
import os
import re
import sys
import threading
import time
import types
import urllib.parse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mdjaada_agent import checks, platforms  # noqa: E402


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


NOW = datetime.now(timezone.utc)


def pod(name, ready=True, restarts=0, waiting=None, oom=False, phase="Running", age_s=600):
    cs = {"name": "app", "restartCount": restarts, "state": {"waiting": {"reason": waiting}} if waiting else {"running": {}}}
    if oom:
        cs["lastState"] = {"terminated": {"reason": "OOMKilled"}}
    return {"metadata": {"name": name, "creationTimestamp": iso(NOW - timedelta(seconds=age_s))},
            "status": {"phase": phase, "containerStatuses": [cs] if phase == "Running" else [],
                       "conditions": [{"type": "Ready", "status": "True" if ready else "False"}] if phase == "Running"
                       else [{"type": "PodScheduled", "status": "False", "reason": "Unschedulable"}]}}


def deployment(replicas=3, ready=3, image="orders:1.0", generation=1, conditions=()):
    return {"metadata": {"name": "orders", "generation": generation},
            "spec": {"replicas": replicas, "selector": {"matchLabels": {"app": "orders"}},
                     "template": {"metadata": {"labels": {"app": "orders", "tier": "api"}},
                                  "spec": {"containers": [{"name": "app", "image": image}]}}},
            "status": {"observedGeneration": generation, "readyReplicas": ready, "updatedReplicas": replicas,
                       "conditions": list(conditions)}}


@pytest.fixture
def k8s(monkeypatch):
    monkeypatch.delenv("KUBECONFIG", raising=False)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    platforms._kube_cache.clear()
    state = {
        "deployment": deployment(),
        "pods": [pod("orders-a"), pod("orders-b"), pod("orders-c")],
        "events": [],
        "auth": [],
        "services": [
            {"metadata": {"name": "kubernetes"}, "spec": {"selector": None, "ports": [{"port": 443}]}},
            {"metadata": {"name": "orders"}, "spec": {"selector": {"app": "orders"}, "ports": [{"port": 8080}]}},
            {"metadata": {"name": "legacy"}, "spec": {"selector": {"app": "legacy"}, "ports": [{"port": 80}]}},
        ],
    }

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            state["auth"].append(self.headers.get("Authorization"))
            url = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(url.query)
            p = url.path
            body, code = None, 200
            if p == "/apis/apps/v1/namespaces/shop/deployments/orders":
                body = state["deployment"]
            elif p.startswith("/apis/apps/v1/namespaces/shop/deployments/"):
                code, body = 404, {"message": f'deployments.apps "{p.rsplit("/", 1)[-1]}" not found'}
            elif p == "/api/v1/namespaces/shop/pods":
                assert q["labelSelector"] == ["app=orders"]
                body = {"items": state["pods"]}
            elif p == "/api/v1/namespaces/shop/events":
                assert q["fieldSelector"] == ["type=Warning"]
                body = {"items": state["events"]}
            elif p == "/api/v1/namespaces/default/services":
                body = {"items": [state["services"][0]]}
            elif p == "/api/v1/namespaces/shop/services":
                body = {"items": state["services"][1:]}
            elif p == "/apis/apps/v1/namespaces/shop/deployments":
                body = {"items": [state["deployment"]]}
            elif p.startswith("/apis/apps/v1/namespaces/"):
                body = {"items": []}
            elif p == "/api/v1/namespaces":
                body = {"items": [{"metadata": {"name": n}} for n in ("default", "shop", "kube-system")]}
            else:
                code, body = 404, {"message": "not found"}
            data = json.dumps(body).encode()
            self.send_response(code)
            self.end_headers()
            self.wfile.write(data)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}", state
    srv.shutdown()


def check(url, node_id="k", **cfg):
    base = {"api_url": url, "token": "t0ken", "namespace": "shop", "name": "orders"}
    return checks.run_check({"node_id": node_id, "type": "kubernetes", "config": {**base, **cfg}})


def test_healthy_deployment(k8s):
    url, state = k8s
    r = check(url)
    assert r.ok and not r.warn and "3/3 ready" in r.message
    assert r.metrics["ready"] == 3 and r.metrics["pods"] == 3 and r.events == []
    assert set(state["auth"]) == {"Bearer t0ken"}


def test_crashloop_restarts_and_events(k8s):
    url, state = k8s
    check(url, node_id="crash")
    state["deployment"] = deployment(ready=2)
    state["pods"] = [pod("orders-a"), pod("orders-b"), pod("orders-c", ready=False, restarts=4, waiting="CrashLoopBackOff")]
    state["events"] = [{"reason": "BackOff", "message": "Back-off restarting failed container app",
                        "involvedObject": {"kind": "Pod", "name": "orders-c"}, "lastTimestamp": iso(NOW)},
                       {"reason": "BackOff", "message": "from a pod an earlier rollout replaced",
                        "involvedObject": {"kind": "Pod", "name": "orders-old"}, "lastTimestamp": iso(NOW)},
                       {"reason": "FailedMount", "message": "other workload", "involvedObject": {"name": "billing-x"},
                        "lastTimestamp": iso(NOW)}]
    r = check(url, node_id="crash")
    assert r.ok and r.warn
    assert "2/3 ready" in r.message and "orders-c: CrashLoopBackOff" in r.message
    assert "restarts since last check 4 > 0" in r.message
    assert "events: BackOff: Back-off restarting failed container app" in r.message and "FailedMount" not in r.message
    assert "earlier rollout" not in r.message

    state["deployment"] = deployment(ready=0)
    state["pods"] = [pod(n, ready=False, restarts=9, waiting="CrashLoopBackOff") for n in ("orders-a", "orders-b", "orders-c")]
    r = check(url, node_id="crash")
    assert not r.ok and "0/3 pods ready" in r.message


def test_scaled_to_zero(k8s):
    url, state = k8s
    state["deployment"] = deployment(replicas=0, ready=0)
    state["pods"] = []
    r = check(url)
    assert r.ok and not r.warn and "deployment/orders scaled to 0" in r.message  # idle on purpose by default
    assert check(url, alert_scaled_to_zero="true").warn
    r = check(url, alert_scaled_to_zero="fail")
    assert not r.ok and "scaled to 0" in r.message
    r = check(url, min_ready="1")  # what discovered Services get
    assert not r.ok and "scaled to 0, below minimum 1" in r.message


def test_rollout_becomes_a_change_event(k8s):
    url, state = k8s
    assert check(url, node_id="roll").events == []
    state["deployment"] = deployment(image="orders:1.1", generation=2)
    r = check(url, node_id="roll")
    assert r.events == [{"kind": "deploy", "message": "rollout of deployment/orders: image orders:1.1"}]
    assert check(url, node_id="roll").events == []  # only once


def test_stuck_rollout_pending_and_oom(k8s):
    url, state = k8s
    state["deployment"] = deployment(conditions=[{"type": "Progressing", "status": "False",
                                                  "reason": "ProgressDeadlineExceeded",
                                                  "message": 'ReplicaSet "orders-7d" has timed out progressing.'}])
    state["pods"] = [pod("orders-a", oom=True), pod("orders-b"), pod("orders-new", phase="Pending", age_s=400)]
    r = check(url)
    assert not r.ok and "rollout stuck: ReplicaSet" in r.message
    # pending for ~400 s when the fixture was built; a slow CI runner adds seconds before the check runs
    assert re.search(r"orders-new: Pending 4\d\ds Unschedulable", r.message), r.message
    assert "OOMKilled recently: orders-a" in r.message


def test_missing_workload_and_credentials(k8s, monkeypatch):
    url, _ = k8s
    r = check(url, name="nope")
    assert not r.ok and 'HTTP 404 deployments.apps "nope" not found' in r.message
    platforms._kube_cache.clear()
    r = checks.run_check({"node_id": "x", "type": "kubernetes", "config": {"namespace": "shop", "name": "orders"}})
    assert not r.ok and "no Kubernetes credentials" in r.message


def test_kubeconfig_token_auth(k8s, tmp_path, monkeypatch):
    pytest.importorskip("yaml")
    url, state = k8s
    kc = tmp_path / "config"
    kc.write_text(f"""
apiVersion: v1
kind: Config
current-context: demo
contexts: [{{name: demo, context: {{cluster: c1, user: u1}}}}]
clusters: [{{name: c1, cluster: {{server: "{url}"}}}}]
users: [{{name: u1, user: {{token: from-kubeconfig}}}}]
""")
    monkeypatch.setenv("KUBECONFIG", str(kc))
    platforms._kube_cache.clear()
    r = checks.run_check({"node_id": "kc", "type": "kubernetes", "config": {"namespace": "shop", "name": "orders"}})
    assert r.ok and state["auth"][-1] == "Bearer from-kubeconfig"


def test_discovery_suggests_services_with_their_workloads(k8s):
    url, _ = k8s
    found = platforms.discover_kubernetes({"api_url": url}, ["all"])
    by_name = {f["name"]: f for f in found}
    assert set(by_name) == {"orders", "legacy"}  # default/kubernetes and kube-system skipped
    assert by_name["orders"]["config"] == {"namespace": "shop", "service": "orders", "host": "orders.shop.svc",
                                           "service_port": "8080", "kind": "deployment", "name": "orders",
                                           "min_ready": "1"}
    assert by_name["legacy"]["config"]["selector"] == "app=legacy" and "name" not in by_name["legacy"]["config"]
    assert by_name["orders"]["key"] == "k8s/shop/service/orders"


# ---------------------------------------------------------------- CloudWatch (fake boto3 client)
class FakeCW:
    def __init__(self, alarms=(), points=()):
        self.alarms, self.points, self.calls = list(alarms), list(points), []

    def describe_alarms(self, **kw):
        self.calls.append(("describe_alarms", kw))
        names = kw.get("AlarmNames")
        return {"MetricAlarms": [a for a in self.alarms if not names or a["AlarmName"] in names], "CompositeAlarms": []}

    def get_metric_statistics(self, **kw):
        self.calls.append(("get_metric_statistics", kw))
        return {"Datapoints": self.points}


def cw(**cfg):
    return checks.run_check({"node_id": f"cw-{time.time_ns()}", "type": "cloudwatch", "config": cfg})


def test_cloudwatch_alarms(monkeypatch):
    fake = FakeCW(alarms=[{"AlarmName": "api-5xx", "StateValue": "OK"},
                          {"AlarmName": "rds-cpu", "StateValue": "ALARM", "StateReason": "Threshold Crossed: 1 datapoint [97.5] > 90"}])
    monkeypatch.setattr(platforms, "_aws", lambda cfg, service: fake)
    r = cw(alarm_names="api-5xx, rds-cpu, gone")
    assert not r.ok and "ALARM: rds-cpu (Threshold Crossed" in r.message and "alarms not found: gone" in r.message
    assert r.metrics == {"alarms": 2, "alarms_firing": 1}
    fake.alarms[1]["StateValue"] = "OK"
    assert cw(alarm_prefix="api").ok


def test_cloudwatch_metric_and_percentiles(monkeypatch):
    now = datetime.now(timezone.utc)
    fake = FakeCW(points=[{"Timestamp": now - timedelta(minutes=2), "ExtendedStatistics": {"p99": 120.0}},
                          {"Timestamp": now - timedelta(minutes=1), "ExtendedStatistics": {"p99": 950.0}}])
    monkeypatch.setattr(platforms, "_aws", lambda cfg, service: fake)
    r = cw(namespace="AWS/ApplicationELB", metric_name="TargetResponseTime", statistic="p99",
           dimensions="LoadBalancer=app/shop/abc, TargetGroup=tg/orders/def", value_warn=500)
    assert r.ok and r.warn and "TargetResponseTime p99 950 > 500" in r.message
    kw = fake.calls[-1][1]
    assert kw["ExtendedStatistics"] == ["p99"] and "Statistics" not in kw
    assert kw["Dimensions"] == [{"Name": "LoadBalancer", "Value": "app/shop/abc"},
                                {"Name": "TargetGroup", "Value": "tg/orders/def"}]
    fake.points = []
    r = cw(namespace="AWS/RDS", metric_name="CPUUtilization", value_fail=90)
    assert r.ok and r.warn and "no AWS/RDS/CPUUtilization datapoints" in r.message


# ---------------------------------------------------------------- Azure Monitor (fake SDK calls)
def az_metrics(values):
    data = [types.SimpleNamespace(average=v, maximum=v) for v in values]
    return types.SimpleNamespace(metrics=[types.SimpleNamespace(timeseries=[types.SimpleNamespace(data=data)])])


class FakeAzLogs:
    def __init__(self, rows):
        self.rows = rows

    def query_workspace(self, workspace, query, **kw):
        assert kw["server_timeout"] and kw["timespan"] == timedelta(hours=1)
        return types.SimpleNamespace(tables=[types.SimpleNamespace(rows=self.rows)], partial_error=None)


def az(**cfg):
    return checks.run_check({"node_id": f"az-{time.time_ns()}", "type": "azure_monitor", "config": cfg})


RID = "/subscriptions/1/resourceGroups/shop/providers/Microsoft.Sql/servers/s1/databases/orders"


def test_azure_metric_namespace_from_resource_id():
    assert platforms.metric_namespace(RID) == "Microsoft.Sql/servers/databases"
    assert platforms.metric_namespace("/subscriptions/1/resourceGroups/g/providers/Microsoft.Web/sites/shop") == "Microsoft.Web/sites"


def test_azure_metric_and_kql(monkeypatch):
    calls, values = [], {"v": [40.0, None, 93.0]}
    monkeypatch.setattr(platforms, "_azure_metrics",
                        lambda cfg, rid, metric, agg: calls.append((rid, metric, agg)) or az_metrics(values["v"]))
    monkeypatch.setattr(platforms, "_azure_logs", lambda: FakeAzLogs([[17]]))
    r = az(resource_id=RID, metric_name="cpu_percent", region="westeurope", value_warn=80)
    assert r.ok and r.warn and "cpu_percent average 93 > 80" in r.message
    assert calls[0] == (RID, "cpu_percent", "Average")

    r = az(workspace_id="ws-1", kql="AppExceptions | where TimeGenerated > ago(5m) | count",
           kql_label="exceptions (5m)", kql_fail=10)
    assert not r.ok and "exceptions (5m) 17 > 10" in r.message

    values["v"] = []
    assert "no cpu_percent data" in az(resource_id=RID, metric_name="cpu_percent").message


def test_azure_missing_sdk_and_region(monkeypatch):
    def no_sdk(*a):
        raise ImportError("no azure")
    monkeypatch.setattr(platforms, "_azure_metrics", no_sdk)
    r = az(resource_id="/x", metric_name="m")
    assert r.ok and r.warn and "install azure-monitor-querymetrics" in r.message
