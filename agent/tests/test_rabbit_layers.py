import json
import os
import sys
import threading
import types
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mdjaada_agent import checks, queues, streams  # noqa: E402


@pytest.fixture
def mgmt(monkeypatch):
    """Fake RabbitMQ management API for a 3-node cluster."""
    t = [1000.0]
    monkeypatch.setattr(queues, "clock", lambda: t[0])
    queues._memory.clear()
    node = lambda name, **kw: {"name": name, "running": True, "mem_alarm": False, "disk_free_alarm": False,  # noqa: E731
                               "partitions": [], "fd_used": 10, "fd_total": 1000, **kw}
    state = {
        "nodes": [node("rabbit@r1"), node("rabbit@r2"), node("rabbit@r3")],
        "queues": {
            "orders": {"name": "orders", "vhost": "/", "messages_ready": 5, "messages_unacknowledged": 1,
                       "consumers": 2, "members": ["rabbit@r1", "rabbit@r2", "rabbit@r3"],
                       "online": ["rabbit@r1", "rabbit@r2", "rabbit@r3"],
                       "arguments": {"x-dead-letter-exchange": "", "x-dead-letter-routing-key": "orders.dlq"},
                       "message_stats": {"publish": 100, "publish_details": {"rate": 5.0}}},
            "orders.dlq": {"name": "orders.dlq", "vhost": "/", "messages": 0, "arguments": {}},
            "amq.gen-x": {"name": "amq.gen-x", "vhost": "/", "arguments": {}},
        },
    }

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            body = None
            if path == "/api/nodes":
                body = state["nodes"]
            elif path == "/api/queues":
                body = list(state["queues"].values())
            elif path.startswith("/api/queues/%2F/"):
                body = state["queues"].get(urllib.parse.unquote(path.rsplit("/", 1)[-1]))
            self.send_response(200 if body is not None else 404)
            self.end_headers()
            self.wfile.write(json.dumps(body if body is not None else {"error": "Object Not Found"}).encode())

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}", state, t
    srv.shutdown()


def run(kind, node_id="n", **cfg):
    return checks.run_check({"node_id": node_id, "type": kind, "config": cfg})


def test_cluster_members_alarms_and_partitions(mgmt):
    url, state, _ = mgmt
    r = run("rabbitmq_cluster", "c", url=url)
    assert r.ok and not r.warn and r.metrics == {"nodes_running": 3, "nodes": 3}
    state["nodes"][2]["running"] = False
    r = run("rabbitmq_cluster", "c", url=url)
    assert r.ok and r.warn and "1 of 3 nodes not running: rabbit@r3" in r.message
    del state["nodes"][2]  # removed from the listing entirely: still remembered as a member
    assert "rabbit@r3" in run("rabbitmq_cluster", "c", url=url).message
    state["nodes"][0]["mem_alarm"] = True
    state["nodes"][1]["partitions"] = ["rabbit@r1"]
    r = run("rabbitmq_cluster", "c", url=url)
    assert not r.ok and "memory alarm on rabbit@r1" in r.message and "network partition: rabbit@r2 cannot see rabbit@r1" in r.message
    for n in state["nodes"]:
        n["running"] = False
    assert not run("rabbitmq_cluster", "c", url=url).ok
    assert "unreachable" in run("rabbitmq_cluster", "x", url="http://127.0.0.1:1").message


def test_queue_consumer_side_producer_side_and_replicas(mgmt):
    url, state, t = mgmt
    q = state["queues"]["orders"]
    cfg = dict(url=url, queue="orders", dlq="orders.dlq", idle_s_warn=60)
    r = run("rabbitmq_queue", "q", **cfg)
    assert r.ok and not r.warn and r.metrics["consumers"] == 2 and r.metrics["dead_letters"] == 0
    q["online"] = ["rabbit@r1", "rabbit@r2"]  # a replica's node is down: the cluster node reports that
    r = run("rabbitmq_queue", "q", **cfg)
    assert r.ok and not r.warn and "1 of 3 replicas offline" in r.message
    q.update(consumers=0, messages_ready=300)
    r = run("rabbitmq_queue", "q", **cfg)
    assert r.warn and "orders has 0 consumers (expected >= 1) (consumer side)" in r.message
    q.update(consumers=2, messages_ready=0)
    t[0] += 90  # nothing published for 90 s
    r = run("rabbitmq_queue", "q", **cfg)
    assert r.warn and "nothing published for 90 s > 60 s" in r.message and "producer side" in r.message
    assert "does not exist" in run("rabbitmq_queue", "q2", url=url, queue="nope").message


def test_discovery_and_inherited_connection(mgmt):
    url, _, _ = mgmt
    found = queues.discover_rabbitmq({"node_id": "rc", "config": {"url": url}})
    assert [f["name"] for f in found] == ["orders"]  # the DLQ and server-named queues are not separate nodes
    assert found[0]["config"] == {"cluster": "rc", "queue": "orders", "vhost": "/", "dlq": "orders.dlq"}
    nodes = {"rc": {"node_id": "rc", "type": "rabbitmq_cluster",
                    "config": {"url": url, "username": "mon", "password": "env:RMQ_PW", "fd_pct_warn": "80"}}}
    merged = streams.with_cluster({"node_id": "q", "type": "rabbitmq_queue", "config": found[0]["config"]}, nodes)
    assert merged["config"]["url"] == url and merged["config"]["password"] == "env:RMQ_PW"
    assert "fd_pct_warn" not in merged["config"]


def test_pubsub_topic_producer_side(monkeypatch):
    values = {"n": [3.0, 4.0]}

    class Fake:
        def list_time_series(self, request, timeout=None, retry=None):
            assert 'pubsub.googleapis.com/topic/send_message_operation_count' in request["filter"]
            assert 'resource.labels.topic_id = "orders"' in request["filter"]
            pts = [types.SimpleNamespace(value=types.SimpleNamespace(int64_value=v, double_value=0)) for v in values["n"]]
            return [types.SimpleNamespace(points=pts)] if pts else []
    monkeypatch.setattr(queues, "_monitoring_client", lambda cfg: Fake())
    r = run("pubsub_topic", project="p", topic="orders")
    assert r.ok and not r.warn and r.metrics["published_15m"] == 7
    values["n"] = []
    r = run("pubsub_topic", project="p", topic="orders")
    assert r.warn and "nothing published to orders" in r.message and "producer side" in r.message
