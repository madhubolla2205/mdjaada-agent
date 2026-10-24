"""Node library, part 5: Kafka as layers.

  kafka_cluster         brokers present, controller, offline / under-replicated partitions
  kafka_topic           messages in per second, "no new messages for N s" (the producer side)
  kafka_consumer_group  state, members, lag, estimated seconds behind, stalled consumption (the consumer side)

Topic and group nodes reference their cluster node (setting `cluster` = the cluster node's id); the agent fills in
the cluster's connection settings before checking them. `discover_kafka()` lists topics and consumer groups as
suggested nodes. Talks to Kafka directly through confluent-kafka (optional driver), behind the small `KafkaAdmin`
adapter so the logic can be tested without a cluster.
"""
import time
from typing import Any, Optional

from .checks import Result, _memory, err, get, num, thresholds

CONNECTION_KEYS = ("bootstrap", "security_protocol", "sasl_mechanism", "sasl_username", "sasl_password",
                   "ssl_ca_location")
INTERNAL_TOPIC_PREFIXES = ("__", "_schemas", "_confluent", "connect-", "_connect")
TIMEOUT = 6.0
clock = time.time  # replaced in tests


# ---------------------------------------------------------------- adapter over confluent-kafka
class KafkaAdmin:
    """The handful of Kafka calls MDJaada needs, returning plain Python data."""

    def __init__(self, cfg: dict):
        from confluent_kafka.admin import AdminClient  # type: ignore
        conf = {"bootstrap.servers": get(cfg, "bootstrap"), "socket.timeout.ms": int(TIMEOUT * 1000),
                "client.id": "mdjaada-agent"}
        for key, conf_key in (("security_protocol", "security.protocol"), ("sasl_mechanism", "sasl.mechanism"),
                              ("sasl_username", "sasl.username"), ("sasl_password", "sasl.password"),
                              ("ssl_ca_location", "ssl.ca.location")):
            if get(cfg, key):
                conf[conf_key] = get(cfg, key)
        self.client = AdminClient(conf)

    def metadata(self) -> dict:
        md = self.client.list_topics(timeout=TIMEOUT)
        return {
            "brokers": {b.id: f"{b.host}:{b.port}" for b in md.brokers.values()},
            "controller": md.controller_id,
            "topics": {name: {p.id: {"leader": p.leader, "replicas": list(p.replicas), "isr": list(p.isrs)}
                              for p in t.partitions.values()}
                       for name, t in md.topics.items() if not t.error},
        }

    def end_offsets(self, topic: str, partitions: list[int]) -> dict[int, int]:
        from confluent_kafka import TopicPartition  # type: ignore
        from confluent_kafka.admin import OffsetSpec  # type: ignore
        futs = self.client.list_offsets({TopicPartition(topic, p): OffsetSpec.latest() for p in partitions},
                                        request_timeout=TIMEOUT)
        return {tp.partition: f.result(timeout=TIMEOUT).offset for tp, f in futs.items()}

    def groups(self) -> list[str]:
        res = self.client.list_consumer_groups(request_timeout=TIMEOUT).result(timeout=TIMEOUT)
        return sorted(g.group_id for g in res.valid)

    def group(self, group_id: str) -> dict:
        desc = self.client.describe_consumer_groups([group_id], request_timeout=TIMEOUT)[group_id].result(timeout=TIMEOUT)
        return {"state": str(desc.state).split(".")[-1].upper(), "members": len(desc.members)}

    def committed(self, group_id: str) -> dict[tuple[str, int], int]:
        from confluent_kafka import ConsumerGroupTopicPartitions  # type: ignore
        futs = self.client.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group_id)],
                                                       request_timeout=TIMEOUT)
        res = futs[group_id].result(timeout=TIMEOUT)
        return {(tp.topic, tp.partition): tp.offset for tp in res.topic_partitions}


_admins: dict[tuple, Any] = {}


def admin(cfg: dict) -> KafkaAdmin:
    key = tuple(str(get(cfg, k) or "") for k in CONNECTION_KEYS)
    if key not in _admins:
        _admins[key] = KafkaAdmin(cfg)
    return _admins[key]


def _admin_or_result(cfg: dict):
    if not get(cfg, "bootstrap"):
        return None, [Result(True, "set the cluster (or its bootstrap servers) to check this node", warn=True)]
    try:
        return admin(cfg), None
    except ImportError:
        return None, [Result(True, "install confluent-kafka on the agent host for Kafka cluster/topic/group checks",
                             warn=True)]
    except Exception as exc:
        return None, [Result(False, f"Kafka client setup failed: {err(exc)}")]


def _since(node_id: str, name: str, value: Any, now: float) -> float:
    """Seconds since `value` last changed (remembered between checks)."""
    key = f"{node_id}:{name}"
    prev = _memory.get(key)
    if not prev or prev[0] != value:
        _memory[key] = (value, now)
        return 0.0
    return now - prev[1]


def _rate(node_id: str, name: str, total: float, now: float) -> Optional[float]:
    key = f"{node_id}:{name}:rate"
    prev = _memory.get(key)
    _memory[key] = (total, now)
    if not prev or now <= prev[1] or total < prev[0]:
        return None
    return (total - prev[0]) / (now - prev[1])


# ---------------------------------------------------------------- cluster
def kafka_cluster(node_id: str, cfg: dict) -> list[Result]:
    a, early = _admin_or_result(cfg)
    if early:
        return early
    try:
        start = time.monotonic()
        md = a.metadata()
        ms = (time.monotonic() - start) * 1000
    except Exception as exc:
        return [Result(False, f"cluster unreachable: {err(exc)}")]
    brokers = md["brokers"]
    seen = _memory.setdefault(f"{node_id}:brokers_seen", {})
    seen.update(brokers)
    expected = int(num(cfg, "expected_brokers") or len(seen))
    missing = [f"{bid} ({addr})" for bid, addr in sorted(seen.items()) if bid not in brokers]
    offline = [f"{t}/{p}" for t, parts in md["topics"].items() for p, info in parts.items() if info["leader"] < 0]
    urp = [f"{t}/{p}" for t, parts in md["topics"].items() for p, info in parts.items()
           if len(info["isr"]) < len(info["replicas"])]
    metrics = {"brokers": len(brokers), "expected_brokers": expected, "offline_partitions": len(offline),
               "under_replicated_partitions": len(urp), "topics": len(md["topics"])}
    parts = [Result(True, f"{len(brokers)}/{expected} brokers", latency_ms=ms, metrics=metrics)]
    if not brokers:
        parts[0] = Result(False, "no brokers in cluster metadata", latency_ms=ms, metrics=metrics)
    elif len(brokers) < expected:
        parts.append(Result(True, f"{expected - len(brokers)} of {expected} brokers missing"
                                  + (f": {', '.join(missing)}" if missing else ""), warn=True))
    if md["controller"] is None or md["controller"] < 0:
        parts.append(Result(False, "no active controller"))
    if offline:
        parts.append(Result(False, f"{len(offline)} offline partitions (no leader): {', '.join(offline[:4])}"))
    if urp:
        r = thresholds(len(urp), {**cfg, "urp_warn": get(cfg, "urp_warn", 0)}, "urp", "under-replicated partitions")
        r.message += f" ({', '.join(urp[:3])}{'…' if len(urp) > 3 else ''})"
        parts.append(r)
    return parts


# ---------------------------------------------------------------- topic
def kafka_topic(node_id: str, cfg: dict) -> list[Result]:
    topic = get(cfg, "topic")
    if not topic:
        return []
    a, early = _admin_or_result(cfg)
    if early:
        return early
    try:
        start = time.monotonic()
        md = a.metadata()
        ms = (time.monotonic() - start) * 1000
        if topic not in md["topics"]:
            return [Result(False, f"topic {topic} does not exist", latency_ms=ms)]
        partitions = md["topics"][topic]
        ends = a.end_offsets(topic, sorted(partitions))
    except Exception as exc:
        return [Result(False, f"topic {topic}: {err(exc)}")]
    now = clock()
    total = sum(ends.values())
    rate = _rate(node_id, "end", total, now)
    idle = _since(node_id, "end", total, now)
    offline = [p for p, info in partitions.items() if info["leader"] < 0]
    urp = [p for p, info in partitions.items() if len(info["isr"]) < len(info["replicas"])]
    metrics = {"partitions": len(partitions), "messages_total": total, "idle_s": round(idle)}
    if rate is not None:
        metrics["messages_per_s"] = round(rate, 2)
    msg = f"{len(partitions)} partitions" + (f", {rate:.1f} msg/s in" if rate is not None else "")
    parts = [Result(True, msg, latency_ms=ms, metrics=metrics)]
    if offline:
        parts.append(Result(False, f"{len(offline)} partitions without a leader: {offline[:5]}"))
    if urp:  # a broker problem, reported on the cluster node: informational here so the topic isn't blamed
        parts[0].message += f", {len(urp)} under-replicated"
        parts[0].metrics["under_replicated"] = len(urp)
    if num(cfg, "idle_s_warn") or num(cfg, "idle_s_fail"):
        r = thresholds(idle, cfg, "idle_s", "no new messages for", " s")
        if not r.ok or r.warn:
            r.message += " (producer side: check the services publishing to this topic)"
            parts.append(r)
    return parts


# ---------------------------------------------------------------- consumer group
GROUP_BAD_STATES = {"DEAD"}
GROUP_REBALANCING = {"PREPARING_REBALANCE", "COMPLETING_REBALANCE", "PREPARINGREBALANCE", "COMPLETINGREBALANCE"}


def kafka_consumer_group(node_id: str, cfg: dict) -> list[Result]:
    group = get(cfg, "group")
    if not group:
        return []
    a, early = _admin_or_result(cfg)
    if early:
        return early
    only = {t.strip() for t in str(get(cfg, "topics", "")).split(",") if t.strip()}
    try:
        start = time.monotonic()
        info = a.group(group)
        ms = (time.monotonic() - start) * 1000
        committed = {tp: off for tp, off in a.committed(group).items() if not only or tp[0] in only}
        ends: dict[tuple[str, int], int] = {}
        for topic in sorted({t for t, _ in committed}):
            for p, off in a.end_offsets(topic, sorted(p for t, p in committed if t == topic)).items():
                ends[(topic, p)] = off
    except Exception as exc:
        return [Result(False, f"group {group}: {err(exc)}")]
    now = clock()
    lag = sum(max(ends.get(tp, off) - off, 0) for tp, off in committed.items() if off >= 0)
    uncommitted = sum(1 for off in committed.values() if off < 0)
    position = sum(off for off in committed.values() if off >= 0)
    rate = _rate(node_id, "committed", position, now)
    stuck_for = _since(node_id, "committed", position, now) if lag > 0 else 0.0
    state, members = info["state"], info["members"]
    metrics = {"state": state, "members": members, "lag": lag, "topics": len({t for t, _ in committed})}
    if rate is not None:
        metrics["consumed_per_s"] = round(rate, 2)
        if rate > 0 and lag:
            metrics["seconds_behind"] = round(lag / rate)

    r = thresholds(float(lag), cfg, "lag", f"{group} lag")
    r.latency_ms, r.metrics = ms, metrics
    parts = [r]
    if "seconds_behind" in metrics:
        r2 = thresholds(metrics["seconds_behind"], cfg, "lag_s", "behind", " s")
        if not r2.ok or r2.warn:
            parts.append(r2)
    want = int(num(cfg, "min_members") or 0)
    if state in GROUP_BAD_STATES:
        parts.append(Result(False, f"group {group} is {state}"))
    elif state == "EMPTY" and lag > 0:
        parts.append(Result(want == 0, f"no active consumers in {group} while {lag} messages wait", warn=True))
    elif members < want:
        parts.append(Result(True, f"{members} consumers (expected >= {want})", warn=True))
    if state in GROUP_REBALANCING:
        since = _since(node_id, "rebalancing", "yes", now)
        parts.append(Result(True, f"rebalancing for {since:.0f} s", warn=since > (num(cfg, "rebalance_s_warn") or 60)))
    else:
        _memory.pop(f"{node_id}:rebalancing", None)
    if lag > 0 and stuck_for:
        st = thresholds(stuck_for, {"stalled_s_warn": 120, "stalled_s_fail": 600, **{k: v for k, v in cfg.items() if v not in (None, "")}},
                        "stalled_s", "consumption stalled for", " s")
        if not st.ok or st.warn:
            st.message += " (consumer side: committed offsets are not moving)"
            parts.append(st)
    if uncommitted:
        parts.append(Result(True, f"{uncommitted} partitions without committed offsets"))
    return parts


# ---------------------------------------------------------------- discovery
def discover_kafka(cluster_node: dict) -> list[dict]:
    """Topics and consumer groups of one cluster node, as suggested nodes linked to it."""
    cfg, cid = cluster_node.get("config") or {}, cluster_node["node_id"]
    a = admin(cfg)
    md = a.metadata()
    topics = sorted(t for t in md["topics"] if not t.startswith(INTERNAL_TOPIC_PREFIXES))[:100]
    found = [{"key": f"kafka/{cid}/topic/{t}", "name": t, "type": "kafka_topic", "source": "kafka",
              "config": {"cluster": cid, "topic": t}} for t in topics]
    for g in a.groups()[:100]:
        group_topics = sorted({t for t, _ in a.committed(g)})
        found.append({"key": f"kafka/{cid}/group/{g}", "name": g, "type": "kafka_consumer_group", "source": "kafka",
                      "config": {"cluster": cid, "group": g, "topics": ",".join(group_topics)}})
    return found


def with_cluster(node: dict, nodes_by_id: dict[str, dict]) -> dict:
    """A topic/group/queue node with its cluster's connection settings filled in (its own settings win)."""
    cfg = node.get("config") or {}
    cluster = nodes_by_id.get(str(cfg.get("cluster") or ""))
    if not cluster:
        return node
    keys = CLUSTER_KEYS.get(node.get("type"), CONNECTION_KEYS)
    inherited = {k: v for k, v in (cluster.get("config") or {}).items() if k in keys}
    return {**node, "config": {**inherited, **{k: v for k, v in cfg.items() if v not in (None, "")}}}


TYPES: dict[str, Any] = {"kafka_cluster": kafka_cluster, "kafka_topic": kafka_topic,
                         "kafka_consumer_group": kafka_consumer_group}
CLUSTER_KEYS = {"kafka_topic": CONNECTION_KEYS, "kafka_consumer_group": CONNECTION_KEYS,
                "rabbitmq_queue": ("url", "username", "password", "token")}
REFERENCES_CLUSTER = set(CLUSTER_KEYS)
