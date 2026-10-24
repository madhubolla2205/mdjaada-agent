import json
import os
import sys
import threading
import time
import types
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mdjaada_agent import checks, queues  # noqa: E402


def run(node_type, node_id=None, **cfg):
    return checks.run_check({"node_id": node_id or f"q-{time.time_ns()}", "type": node_type, "config": cfg})


# ---------------------------------------------------------------- RabbitMQ (fake management API)
@pytest.fixture
def rabbit():
    state = {
        "nodes": [{"name": "rabbit@a", "running": True, "mem_alarm": False, "disk_free_alarm": False,
                   "fd_used": 50, "fd_total": 1000}],
        "queues": {"notifications": {"messages_ready": 3, "messages_unacknowledged": 1, "consumers": 2,
                                     "message_stats": {"publish_details": {"rate": 4.0},
                                                       "deliver_get_details": {"rate": 4.0}}},
                   "notifications.dlq": {"messages": 0}},
        "seen": [],
    }

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            state["seen"].append((self.path, self.headers.get("Authorization")))
            if self.path == "/api/overview":
                body = {"rabbitmq_version": "3.13.7"}
            elif self.path == "/api/nodes":
                body = state["nodes"]
            elif self.path.startswith("/api/queues/%2F/"):
                name = self.path.rsplit("/", 1)[-1]
                body = state["queues"].get(name)
            else:
                body = None
            self.send_response(200 if body is not None else 404)
            self.end_headers()
            self.wfile.write(json.dumps(body or {"error": "Object Not Found"}).encode())

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}", state
    srv.shutdown()


def test_rabbitmq_healthy_queue(rabbit):
    url, state = rabbit
    r = run("rabbitmq", url=url, username="mdjaada", password="pw", queue="notifications", dlq="notifications.dlq")
    assert r.ok and not r.warn
    assert r.metrics["consumers"] == 2 and r.metrics["backlog"] == 3 and r.metrics["dead_letters"] == 0
    assert r.metrics["nodes_running"] == 1
    assert all(auth and auth.startswith("Basic ") for _, auth in state["seen"])


def test_rabbitmq_no_consumers_growing_backlog_and_dlq(rabbit):
    url, state = rabbit
    cfg = dict(url=url, queue="notifications", dlq="notifications.dlq", backlog_fail=500)
    run("rabbitmq", node_id="rmq", **cfg)
    state["queues"]["notifications"].update(consumers=0, messages_ready=120)
    state["queues"]["notifications.dlq"]["messages"] = 7
    r = run("rabbitmq", node_id="rmq", **cfg)
    assert r.ok and r.warn
    assert "has 0 consumers (expected >= 1)" in r.message
    assert "7 messages in dead-letter queue notifications.dlq" in r.message
    assert r.metrics["backlog_change"] == 117
    state["queues"]["notifications"]["messages_ready"] = 900
    r = run("rabbitmq", node_id="rmq", **cfg)
    assert not r.ok and "notifications backlog 900 > 500 (+780 since last check)" in r.message


def test_rabbitmq_alarms_missing_queue_and_down(rabbit):
    url, state = rabbit
    state["nodes"][0]["mem_alarm"] = True
    r = run("rabbitmq", url=url, queue="nope")
    assert not r.ok and "memory alarm on rabbit@a" in r.message and "queue nope does not exist" in r.message
    r = run("rabbitmq", url="http://127.0.0.1:1")
    assert not r.ok and "management API failed" in r.message


# ---------------------------------------------------------------- AWS SQS (fake boto3 clients)
class FakeSQS:
    def __init__(self, attrs, dlq_count=0, fail=None):
        self.attrs, self.dlq_count, self.fail = attrs, dlq_count, fail

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        if self.fail:
            raise self.fail
        if QueueUrl.endswith("orders-dlq"):
            return {"Attributes": {"ApproximateNumberOfMessages": str(self.dlq_count)}}
        return {"Attributes": self.attrs}

    def get_queue_url(self, QueueName):
        return {"QueueUrl": f"https://sqs.eu-west-1.amazonaws.com/123/{QueueName}"}


class FakeCloudWatch:
    def __init__(self, age):
        self.age, self.calls = age, []

    def get_metric_statistics(self, **kw):
        self.calls.append(kw)
        now = datetime.now(timezone.utc)
        return {"Datapoints": [{"Timestamp": now - timedelta(minutes=2), "Maximum": 5.0},
                               {"Timestamp": now - timedelta(minutes=1), "Maximum": self.age}]}


URL = "https://sqs.eu-west-1.amazonaws.com/123/orders"
ATTRS = {"ApproximateNumberOfMessages": "40", "ApproximateNumberOfMessagesNotVisible": "3",
         "ApproximateNumberOfMessagesDelayed": "0",
         "RedrivePolicy": json.dumps({"deadLetterTargetArn": "arn:aws:sqs:eu-west-1:123:orders-dlq", "maxReceiveCount": 5})}


def test_sqs_backlog_age_and_dlq(monkeypatch):
    cw = FakeCloudWatch(age=30)
    fake = {"sqs": FakeSQS(ATTRS)}
    monkeypatch.setattr(queues, "_aws_clients", lambda cfg: (fake["sqs"], cw))
    r = run("sqs", queue_url=URL, region="eu-west-1", age_s_warn=300)
    assert r.ok and not r.warn and r.metrics["backlog"] == 40 and r.metrics["oldest_message_age_s"] == 30
    assert cw.calls[0]["Dimensions"] == [{"Name": "QueueName", "Value": "orders"}]

    cw.age = 1200
    fake["sqs"] = FakeSQS(ATTRS, dlq_count=12)
    r = run("sqs", queue_url=URL, age_s_warn=300, age_s_fail=900)
    assert not r.ok and "oldest message 1200 s > 900 s" in r.message and "12 messages in dead-letter queue orders-dlq" in r.message


def test_sqs_errors_and_missing_driver(monkeypatch):
    class AccessDenied(Exception):
        pass
    monkeypatch.setattr(queues, "_aws_clients",
                        lambda cfg: (FakeSQS(ATTRS, fail=AccessDenied("AccessDenied: not authorized")), FakeCloudWatch(1)))
    r = run("sqs", queue_url=URL)
    assert not r.ok and "SQS orders: AccessDenied" in r.message

    monkeypatch.undo()
    monkeypatch.setitem(sys.modules, "boto3", None)
    r = run("sqs", queue_url=URL)
    assert r.ok and r.warn and "install boto3" in r.message
    assert checks.DEFAULT_INTERVALS["sqs"] == 60


# ---------------------------------------------------------------- Google Pub/Sub (fake Cloud Monitoring client)
class FakeMonitoring:
    def __init__(self, values):
        self.values, self.requests = values, []

    def list_time_series(self, request, timeout=None, retry=None):
        assert timeout and retry is None  # a check must never hang on Cloud Monitoring
        self.requests.append(request)
        metric = request["filter"].split("subscription/")[1].split('"')[0]
        if metric not in self.values:
            return []
        point = types.SimpleNamespace(value=types.SimpleNamespace(int64_value=self.values[metric], double_value=0))
        return [types.SimpleNamespace(points=[point])]


def test_pubsub_backlog_and_age(monkeypatch):
    fake = FakeMonitoring({"num_undelivered_messages": 2500, "oldest_unacked_message_age": 45})
    monkeypatch.setattr(queues, "_monitoring_client", lambda cfg: fake)
    r = run("pubsub", project="shop-prod", subscription="orders-sub", backlog_warn=1000, age_s_fail=600)
    assert r.ok and r.warn and "orders-sub backlog 2500 > 1000" in r.message
    assert r.metrics["oldest_unacked_age_s"] == 45
    assert fake.requests[0]["name"] == "projects/shop-prod"
    assert 'resource.labels.subscription_id = "orders-sub"' in fake.requests[0]["filter"]

    fake.values["oldest_unacked_message_age"] = 3600
    assert not run("pubsub", project="shop-prod", subscription="orders-sub", age_s_fail=600).ok


def test_pubsub_no_data_is_a_warning(monkeypatch):
    monkeypatch.setattr(queues, "_monitoring_client", lambda cfg: FakeMonitoring({}))
    r = run("pubsub", project="p", subscription="ghost")
    assert r.ok and r.warn and "no metrics for subscription ghost" in r.message


@pytest.mark.parametrize("first", ["queues", "stores", "checks", "main"])
def test_any_module_can_be_imported_first(first):
    import subprocess
    code = (f"import mdjaada_agent.{first}; from mdjaada_agent import checks; "
            "assert {'rabbitmq', 'redis', 'kafka'} <= set(checks.type_checks())")
    agent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    subprocess.run([sys.executable, "-c", code], cwd=agent_dir, check=True)
