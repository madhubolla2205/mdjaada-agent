import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mdjaada_agent import checks, streams  # noqa: E402


class FakeAdmin:
    """In-memory Kafka: brokers, topic partitions with leaders/ISR, end offsets, groups with commits."""

    def __init__(self):
        self.brokers = {1: "k1:9092", 2: "k2:9092", 3: "k3:9092"}
        self.controller = 1
        self.parts = {"orders": {0: (1, [1, 2, 3], [1, 2, 3]), 1: (2, [2, 3, 1], [2, 3, 1])},
                      "payments": {0: (3, [3, 1, 2], [3, 1, 2])},
                      "__consumer_offsets": {0: (1, [1], [1])}}
        self.ends = {("orders", 0): 100, ("orders", 1): 100, ("payments", 0): 50}
        self.groups_ = {"billing": {"state": "STABLE", "members": 2}}
        self.commits = {"billing": {("orders", 0): 90, ("orders", 1): 100}}
        self.down = False

    def metadata(self):
        if self.down:
            raise RuntimeError("KafkaError{code=_TRANSPORT,val=-195,str=\"Failed to get metadata: Local: Broker transport failure\"}")
        return {"brokers": dict(self.brokers), "controller": self.controller,
                "topics": {t: {p: {"leader": l, "replicas": r, "isr": i} for p, (l, r, i) in ps.items()}
                           for t, ps in self.parts.items()}}

    def end_offsets(self, topic, partitions):
        return {p: self.ends[(topic, p)] for p in partitions}

    def groups(self):
        return sorted(self.groups_)

    def group(self, gid):
        return dict(self.groups_[gid])

    def committed(self, gid):
        return dict(self.commits.get(gid, {}))


@pytest.fixture
def kafka(monkeypatch):
    fake, t = FakeAdmin(), [1000.0]
    monkeypatch.setattr(streams, "admin", lambda cfg: fake)
    monkeypatch.setattr(streams, "clock", lambda: t[0])
    streams._memory.clear()
    return fake, t


def run(kind, node_id="n", **cfg):
    return checks.run_check({"node_id": node_id, "type": kind, "config": {"bootstrap": "k1:9092", **cfg}})


def test_cluster_healthy_then_a_broker_disappears(kafka):
    fake, _ = kafka
    r = run("kafka_cluster", "c")
    assert r.ok and not r.warn and r.metrics["brokers"] == 3 and r.metrics["offline_partitions"] == 0
    del fake.brokers[3]  # broker 3 stops: it vanishes from metadata, its replicas leave the ISR
    fake.parts["orders"][0] = (1, [1, 2, 3], [1, 2])
    t = run("kafka_topic", "tt", topic="orders")
    assert t.ok and not t.warn and "1 under-replicated" in t.message  # the cluster's problem, not the topic's
    r = run("kafka_cluster", "c")
    assert r.ok and r.warn
    assert "1 of 3 brokers missing: 3 (k3:9092)" in r.message  # remembered, not configured
    assert "under-replicated partitions 1 > 0 (orders/0)" in r.message


def test_cluster_offline_partitions_no_controller_and_unreachable(kafka):
    fake, _ = kafka
    fake.parts["payments"][0] = (-1, [3, 1, 2], [])
    fake.controller = -1
    r = run("kafka_cluster", "c2")
    assert not r.ok and "1 offline partitions (no leader): payments/0" in r.message and "no active controller" in r.message
    fake.down = True
    r = run("kafka_cluster", "c3")
    assert not r.ok and r.message.startswith("cluster unreachable") and "Broker transport failure" in r.message


def test_topic_rate_and_producer_side_idle(kafka):
    fake, t = kafka
    r = run("kafka_topic", "t", topic="orders", idle_s_warn=60, idle_s_fail=300)
    assert r.ok and not r.warn and r.metrics["partitions"] == 2
    t[0] += 10
    fake.ends[("orders", 0)] += 30
    fake.ends[("orders", 1)] += 20
    r = run("kafka_topic", "t", topic="orders", idle_s_warn=60, idle_s_fail=300)
    assert r.metrics["messages_per_s"] == 5.0 and "5.0 msg/s in" in r.message
    t[0] += 90  # nothing new for 90 s
    r = run("kafka_topic", "t", topic="orders", idle_s_warn=60, idle_s_fail=300)
    assert r.ok and r.warn and "no new messages for 90 s > 60 s" in r.message and "producer side" in r.message
    t[0] += 300
    assert not run("kafka_topic", "t", topic="orders", idle_s_warn=60, idle_s_fail=300).ok
    assert "does not exist" in run("kafka_topic", "x", topic="nope").message


def test_consumer_group_lag_speed_and_stall(kafka):
    fake, t = kafka
    r = run("kafka_consumer_group", "g", group="billing", lag_warn=50)
    assert r.ok and not r.warn and r.metrics["lag"] == 10 and r.metrics["state"] == "STABLE"
    t[0] += 10  # consumers advance 20 while producers add 40: falling behind
    fake.commits["billing"][("orders", 0)] += 20
    fake.ends[("orders", 0)] += 40
    r = run("kafka_consumer_group", "g", group="billing", lag_warn=50)
    assert r.metrics["consumed_per_s"] == 2.0 and r.metrics["lag"] == 30 and r.metrics["seconds_behind"] == 15
    t[0] += 150  # offsets stop moving while messages wait
    r = run("kafka_consumer_group", "g", group="billing", lag_warn=50)
    assert r.ok and r.warn and "consumption stalled for 150 s > 120 s" in r.message and "consumer side" in r.message
    t[0] += 600
    assert not run("kafka_consumer_group", "g", group="billing", lag_warn=50).ok


def test_consumer_group_states(kafka):
    fake, _ = kafka
    fake.groups_["billing"] = {"state": "EMPTY", "members": 0}
    r = run("kafka_consumer_group", "g1", group="billing")
    assert r.ok and r.warn and "no active consumers in billing while 10 messages wait" in r.message
    assert not run("kafka_consumer_group", "g2", group="billing", min_members=1).ok
    fake.groups_["billing"] = {"state": "DEAD", "members": 0}
    assert "is DEAD" in run("kafka_consumer_group", "g3", group="billing").message


def test_discovery_and_cluster_settings(kafka):
    fake, _ = kafka
    found = streams.discover_kafka({"node_id": "cl", "config": {"bootstrap": "k1:9092"}})
    by_key = {f["key"]: f for f in found}
    assert set(by_key) == {"kafka/cl/topic/orders", "kafka/cl/topic/payments", "kafka/cl/group/billing"}  # no internals
    assert by_key["kafka/cl/group/billing"]["config"] == {"cluster": "cl", "group": "billing", "topics": "orders"}
    nodes = {"cl": {"node_id": "cl", "type": "kafka_cluster",
                    "config": {"bootstrap": "k1:9092", "sasl_username": "env:K_USER", "expected_brokers": "3"}}}
    merged = streams.with_cluster({"node_id": "g", "type": "kafka_consumer_group",
                                   "config": {"cluster": "cl", "group": "billing", "lag_warn": "5"}}, nodes)
    assert merged["config"] == {"bootstrap": "k1:9092", "sasl_username": "env:K_USER", "cluster": "cl",
                                "group": "billing", "lag_warn": "5"}  # connection only, not the cluster's thresholds


def test_without_driver_or_cluster(monkeypatch):
    monkeypatch.setitem(sys.modules, "confluent_kafka", None)
    monkeypatch.setitem(sys.modules, "confluent_kafka.admin", None)
    streams._admins.clear()
    r = checks.run_check({"node_id": "z", "type": "kafka_cluster", "config": {"bootstrap": "k:9092"}})
    assert r.ok and r.warn and "install confluent-kafka" in r.message
    r = checks.run_check({"node_id": "z", "type": "kafka_topic", "config": {"topic": "orders"}})
    assert r.warn and "set the cluster" in r.message
