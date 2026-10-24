"""Node library, part 3: message queues (Kafka lives in checks.py).

| type     | how it is checked                                  | driver (optional, on the agent host)       |
|----------|----------------------------------------------------|--------------------------------------------|
| rabbitmq | management HTTP API (:15672)                       | none (built in)                            |
| sqs      | SQS queue attributes + CloudWatch oldest-msg age   | boto3 (standard AWS credential chain)      |
| pubsub   | Cloud Monitoring subscription backlog metrics      | google-cloud-monitoring (ADC credentials)  |

The same questions for every queue: is the broker healthy, is the backlog growing, is anyone consuming,
how old is the oldest message, and is the dead-letter queue filling up.
"""
import json
import time
import urllib.parse
from typing import Any, Optional

from .checks import TIMEOUT, Result, _memory, err, get, http_get, num, thresholds, truthy


def backlog(node_id: str, cfg: dict, count: float, label: str = "backlog") -> Result:
    """Backlog thresholds plus the change since the previous check (a growing backlog = consumers falling behind)."""
    prev = _memory.get(f"{node_id}:backlog")
    _memory[f"{node_id}:backlog"] = count
    r = thresholds(count, cfg, "backlog", label)
    r.metrics = {"backlog": count}
    if prev is not None:
        r.metrics["backlog_change"] = count - prev
        if count > prev:
            r.message += f" (+{count - prev:g} since last check)"
    return r


def dead_letters(count: float, cfg: dict, name: str) -> Result:
    limit = num(cfg, "dlq_warn") or 0
    if count > limit:
        return Result(True, f"{count:g} messages in dead-letter queue {name}", warn=True, metrics={"dead_letters": count})
    return Result(True, f"dead-letter queue {name} empty" if not count else f"{count:g} dead letters",
                  metrics={"dead_letters": count})


# ---------------------------------------------------------------- RabbitMQ
def _rmq(cfg: dict, path: str):
    status, raw, ms = http_get(get(cfg, "url", "").rstrip("/") + path, cfg)
    return json.loads(raw), ms


def _rmq_queue(cfg: dict, name: str) -> Optional[dict]:
    vhost = urllib.parse.quote(get(cfg, "vhost", "/"), safe="")
    try:
        q, _ = _rmq(cfg, f"/api/queues/{vhost}/{urllib.parse.quote(name, safe='')}")
        return q
    except Exception as exc:
        if "404" in str(exc):
            return None
        raise


def rabbitmq(node_id: str, cfg: dict) -> list[Result]:
    if not get(cfg, "url"):
        return []
    try:
        _, ms = _rmq(cfg, "/api/overview")
    except Exception as exc:
        return [Result(False, f"management API failed: {err(exc)}")]
    r = thresholds(ms, cfg, "latency_ms", "management API", " ms")
    r.latency_ms = ms
    parts = [r]

    nodes, _ = _rmq(cfg, "/api/nodes")
    down = [n["name"] for n in nodes if not n.get("running")]
    if down:
        parts.append(Result(False, f"broker nodes not running: {', '.join(down)}"))
    for n in nodes:
        if n.get("mem_alarm"):
            parts.append(Result(False, f"memory alarm on {n['name']}: publishers are blocked"))
        if n.get("disk_free_alarm"):
            parts.append(Result(False, f"disk alarm on {n['name']}: publishers are blocked"))
        fd = (n.get("fd_used") or 0) / (n.get("fd_total") or 1) * 100
        if fd > (num(cfg, "fd_pct_warn") or 90):
            parts.append(Result(True, f"{n['name']} uses {fd:.0f}% of file descriptors", warn=True))
    parts[0].metrics["nodes_running"] = len(nodes) - len(down)

    queue = get(cfg, "queue")
    if queue:
        q = _rmq_queue(cfg, queue)
        if q is None:
            parts.append(Result(False, f"queue {queue} does not exist"))
        else:
            parts.append(backlog(node_id, cfg, float(q.get("messages_ready", 0)), f"{queue} backlog"))
            unacked = float(q.get("messages_unacknowledged", 0))
            r = thresholds(unacked, cfg, "unacked", "unacked")
            r.metrics = {"unacked": unacked}
            parts.append(r)
            consumers = int(q.get("consumers", 0))
            want = int(num(cfg, "consumers_min") if num(cfg, "consumers_min") is not None else 1)
            stats = q.get("message_stats") or {}
            rates = {"publish_rate": (stats.get("publish_details") or {}).get("rate", 0.0),
                     "deliver_rate": (stats.get("deliver_get_details") or {}).get("rate", 0.0)}
            if consumers < want:
                parts.append(Result(True, f"{queue} has {consumers} consumers (expected >= {want})", warn=True,
                                    metrics={"consumers": consumers, **rates}))
            else:
                parts.append(Result(True, f"{consumers} consumers", metrics={"consumers": consumers, **rates}))
    dlq = get(cfg, "dlq")
    if dlq:
        q = _rmq_queue(cfg, dlq)
        if q is not None:
            parts.append(dead_letters(float(q.get("messages", 0)), cfg, dlq))
    return parts


# ---------------------------------------------------------------- AWS SQS
def _aws_clients(cfg: dict):
    from .cloud import aws_client
    return aws_client(cfg, "sqs"), aws_client(cfg, "cloudwatch")


def sqs(node_id: str, cfg: dict) -> list[Result]:
    url = get(cfg, "queue_url")
    if not url:
        return []
    try:
        client, cloudwatch = _aws_clients(cfg)
    except ImportError:
        return [Result(True, "install boto3 on the agent host to enable SQS checks", warn=True)]
    except Exception as exc:  # e.g. NoRegionError
        return [Result(False, f"AWS client setup failed: {err(exc)}")]
    name = url.rstrip("/").rsplit("/", 1)[-1]
    try:
        start = time.monotonic()
        attrs = client.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
        ms = (time.monotonic() - start) * 1000
    except Exception as exc:
        return [Result(False, f"SQS {name}: {err(exc)}")]
    r = thresholds(ms, cfg, "latency_ms", "API latency", " ms")
    r.latency_ms = ms
    parts = [r, backlog(node_id, cfg, float(attrs.get("ApproximateNumberOfMessages", 0)), f"{name} backlog")]
    in_flight = float(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    r = thresholds(in_flight, cfg, "in_flight", "in flight")
    r.metrics = {"in_flight": in_flight, "delayed": float(attrs.get("ApproximateNumberOfMessagesDelayed", 0))}
    parts.append(r)

    if not get(cfg, "use_cloudwatch") or truthy(get(cfg, "use_cloudwatch")):
        try:
            now = time.time()
            stats = cloudwatch.get_metric_statistics(
                Namespace="AWS/SQS", MetricName="ApproximateAgeOfOldestMessage",
                Dimensions=[{"Name": "QueueName", "Value": name}], StartTime=now - 900, EndTime=now,
                Period=60, Statistics=["Maximum"])
            points = sorted(stats.get("Datapoints", []), key=lambda p: p["Timestamp"])
            if points:
                age = float(points[-1]["Maximum"])
                r = thresholds(age, cfg, "age_s", "oldest message", " s")
                r.metrics = {"oldest_message_age_s": age}
                parts.append(r)
        except Exception as exc:
            parts.append(Result(True, f"CloudWatch age unavailable: {err(exc)}", warn=True))

    policy = attrs.get("RedrivePolicy")
    if policy:
        dlq_name = json.loads(policy).get("deadLetterTargetArn", "").rsplit(":", 1)[-1]
        try:
            dlq_url = client.get_queue_url(QueueName=dlq_name)["QueueUrl"]
            dlq_attrs = client.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["ApproximateNumberOfMessages"])
            parts.append(dead_letters(float(dlq_attrs["Attributes"].get("ApproximateNumberOfMessages", 0)), cfg, dlq_name))
        except Exception as exc:
            parts.append(Result(True, f"dead-letter queue {dlq_name} unreadable: {err(exc)}", warn=True))
    return parts


# ---------------------------------------------------------------- Google Cloud Pub/Sub
def _monitoring_client(cfg: dict):
    from google.cloud import monitoring_v3  # type: ignore
    return monitoring_v3.MetricServiceClient()


def _latest_point(client, project: str, subscription: str, metric: str) -> Optional[float]:
    now = int(time.time())
    series = client.list_time_series(request={
        "name": f"projects/{project}",
        "filter": f'metric.type = "pubsub.googleapis.com/subscription/{metric}" '
                  f'AND resource.labels.subscription_id = "{subscription}"',
        "interval": {"start_time": {"seconds": now - 600}, "end_time": {"seconds": now}},
    }, timeout=TIMEOUT * 2, retry=None)
    for ts in series:
        for point in ts.points:  # newest first
            v = point.value
            return float(getattr(v, "int64_value", 0) or getattr(v, "double_value", 0) or 0)
    return None


def pubsub(node_id: str, cfg: dict) -> list[Result]:
    project, sub = get(cfg, "project"), get(cfg, "subscription")
    if not (project and sub):
        return []
    try:
        client = _monitoring_client(cfg)
    except ImportError:
        return [Result(True, "install google-cloud-monitoring on the agent host to enable Pub/Sub checks", warn=True)]
    except Exception as exc:  # e.g. DefaultCredentialsError: no GOOGLE_APPLICATION_CREDENTIALS
        return [Result(False, f"Google Cloud credentials: {err(exc)}")]
    try:
        start = time.monotonic()
        undelivered = _latest_point(client, project, sub, "num_undelivered_messages")
        ms = (time.monotonic() - start) * 1000
        age = _latest_point(client, project, sub, "oldest_unacked_message_age")
        dead = _latest_point(client, project, sub, "dead_letter_message_count") if truthy(get(cfg, "check_dlq")) else None
    except Exception as exc:
        return [Result(False, f"Cloud Monitoring query failed: {err(exc)}")]
    if undelivered is None and age is None:
        return [Result(True, f"no metrics for subscription {sub} in the last 10 min (missing or idle?)", warn=True,
                       latency_ms=ms)]
    parts = [backlog(node_id, cfg, undelivered or 0, f"{sub} backlog")]
    parts[0].latency_ms = ms
    if age is not None:
        r = thresholds(age, cfg, "age_s", "oldest unacked", " s")
        r.metrics = {"oldest_unacked_age_s": age}
        parts.append(r)
    if dead is not None:
        parts.append(dead_letters(dead, cfg, f"{sub} (forwarded)"))
    return parts


# ---------------------------------------------------------------- RabbitMQ as layers: cluster -> queue
def _since(node_id: str, name: str, value: Any, now: float) -> float:
    key = f"{node_id}:{name}"
    prev = _memory.get(key)
    if not prev or prev[0] != value:
        _memory[key] = (value, now)
        return 0.0
    return now - prev[1]


clock = time.time  # replaced in tests


def rabbitmq_cluster(node_id: str, cfg: dict) -> list[Result]:
    """Cluster members running (members that drop out are learned, no count needed), alarms, partitions."""
    if not get(cfg, "url"):
        return []
    try:
        nodes, ms = _rmq(cfg, "/api/nodes")
    except Exception as exc:
        return [Result(False, f"management API unreachable: {err(exc)}")]
    seen = _memory.setdefault(f"{node_id}:members", set())
    seen.update(n["name"] for n in nodes)
    running = [n for n in nodes if n.get("running")]
    listed = {n["name"] for n in nodes}
    stopped = sorted(n["name"] for n in nodes if not n.get("running")) + sorted(seen - listed)
    metrics = {"nodes_running": len(running), "nodes": len(seen)}
    if not running:
        return [Result(False, "no cluster node is running", latency_ms=ms, metrics=metrics)]
    parts = [Result(True, f"{len(running)}/{len(seen)} nodes running", latency_ms=ms, metrics=metrics)]
    if stopped:
        parts.append(Result(True, f"{len(stopped)} of {len(seen)} nodes not running: {', '.join(stopped)}", warn=True))
    for n in running:
        if n.get("mem_alarm"):
            parts.append(Result(False, f"memory alarm on {n['name']}: publishers are blocked"))
        if n.get("disk_free_alarm"):
            parts.append(Result(False, f"disk alarm on {n['name']}: publishers are blocked"))
        if n.get("partitions"):
            parts.append(Result(False, f"network partition: {n['name']} cannot see {', '.join(n['partitions'])}"))
        fd = (n.get("fd_used") or 0) / (n.get("fd_total") or 1) * 100
        if fd > (num(cfg, "fd_pct_warn") or 90):
            parts.append(Result(True, f"{n['name']} uses {fd:.0f}% of file descriptors", warn=True))
    return parts


def rabbitmq_queue(node_id: str, cfg: dict) -> list[Result]:
    """One queue: backlog and growth, consumers (consumer side), publishing stopped (producer side), dead letters."""
    queue = get(cfg, "queue")
    if not queue:
        return []
    if not get(cfg, "url"):
        return [Result(True, "set the cluster (or the management URL) to check this queue", warn=True)]
    try:
        start = time.monotonic()
        q = _rmq_queue(cfg, queue)
        ms = (time.monotonic() - start) * 1000
    except Exception as exc:
        return [Result(False, f"queue {queue}: {err(exc)}")]
    if q is None:
        return [Result(False, f"queue {queue} does not exist")]
    now = clock()
    stats = q.get("message_stats") or {}
    published = stats.get("publish")
    consumers = int(q.get("consumers", 0))
    ready = float(q.get("messages_ready", 0))
    parts = [backlog(node_id, cfg, ready, f"{queue} backlog")]
    parts[0].latency_ms = ms
    parts[0].metrics.update(consumers=consumers, unacked=float(q.get("messages_unacknowledged", 0)),
                            publish_rate=(stats.get("publish_details") or {}).get("rate", 0.0),
                            deliver_rate=(stats.get("deliver_get_details") or {}).get("rate", 0.0))
    unacked = thresholds(float(q.get("messages_unacknowledged", 0)), cfg, "unacked", "unacked")
    if not unacked.ok or unacked.warn:
        parts.append(unacked)
    want = int(num(cfg, "consumers_min") if num(cfg, "consumers_min") is not None else 1)
    if consumers < want:
        side = " (consumer side)" if ready else ""
        parts.append(Result(True, f"{queue} has {consumers} consumers (expected >= {want}){side}", warn=True))
    if published is not None and (num(cfg, "idle_s_warn") or num(cfg, "idle_s_fail")):
        idle = thresholds(_since(node_id, "published", published, now), cfg, "idle_s", "nothing published for", " s")
        if not idle.ok or idle.warn:
            idle.message += " (producer side: check the services publishing to this queue)"
            parts.append(idle)
    members, online = q.get("members") or [], q.get("online") or []
    if members and len(online) < len(members):  # quorum replica offline: the cluster node reports the cause
        parts[0].message += f", {len(members) - len(online)} of {len(members)} replicas offline"
    dlq = get(cfg, "dlq")
    if dlq:
        d = _rmq_queue(cfg, dlq)
        if d is not None:
            parts.append(dead_letters(float(d.get("messages", 0)), cfg, dlq))
    return parts


def discover_rabbitmq(cluster_node: dict) -> list[dict]:
    """Queues of one RabbitMQ cluster node, as suggested nodes linked to it."""
    cfg, cid = cluster_node.get("config") or {}, cluster_node["node_id"]
    queues, _ = _rmq(cfg, "/api/queues?columns=name,vhost,arguments")
    dlqs = {(q.get("arguments") or {}).get("x-dead-letter-routing-key") for q in queues} - {None}
    found = []
    for q in queues[:200]:
        if q["name"] in dlqs or q["name"].startswith("amq."):  # DLQs are watched by their main queue
            continue
        config = {"cluster": cid, "queue": q["name"], "vhost": q.get("vhost", "/")}
        dlq = (q.get("arguments") or {}).get("x-dead-letter-routing-key")
        if dlq:
            config["dlq"] = dlq
        found.append({"key": f"rabbitmq/{cid}/{q.get('vhost', '/')}/{q['name']}", "name": q["name"],
                      "type": "rabbitmq_queue", "source": "rabbitmq", "config": config})
    return found


# ---------------------------------------------------------------- Pub/Sub topic (the producer side)
def _latest_topic_point(client, project: str, topic: str, metric: str) -> Optional[float]:
    now = int(time.time())
    series = client.list_time_series(request={
        "name": f"projects/{project}",
        "filter": f'metric.type = "pubsub.googleapis.com/topic/{metric}" AND resource.labels.topic_id = "{topic}"',
        "interval": {"start_time": {"seconds": now - 900}, "end_time": {"seconds": now}},
    }, timeout=TIMEOUT * 2, retry=None)
    total, seen = 0.0, False
    for ts in series:
        for point in ts.points:  # sum of messages published per minute over the window
            v = point.value
            total += float(getattr(v, "int64_value", 0) or getattr(v, "double_value", 0) or 0)
            seen = True
    return total if seen else None


def pubsub_topic(node_id: str, cfg: dict) -> list[Result]:
    project, topic = get(cfg, "project"), get(cfg, "topic")
    if not (project and topic):
        return []
    try:
        client = _monitoring_client(cfg)
    except ImportError:
        return [Result(True, "install google-cloud-monitoring on the agent host to enable Pub/Sub checks", warn=True)]
    except Exception as exc:
        return [Result(False, f"Google Cloud credentials: {err(exc)}")]
    try:
        published = _latest_topic_point(client, project, topic, "send_message_operation_count")
    except Exception as exc:
        return [Result(False, f"Cloud Monitoring query failed: {err(exc)}")]
    count = published or 0.0
    r = thresholds(count, cfg, "published", f"{topic} published (15 min)")
    r.metrics = {"published_15m": count}
    if count == 0 and not truthy(get(cfg, "allow_idle")):
        r.warn = True
        r.message = f"nothing published to {topic} in 15 min (producer side: check the publishing services)"
    return [r]


TYPES: dict[str, Any] = {"rabbitmq": rabbitmq, "sqs": sqs, "pubsub": pubsub, "rabbitmq_cluster": rabbitmq_cluster,
                         "rabbitmq_queue": rabbitmq_queue, "pubsub_topic": pubsub_topic}
# cloud metrics have 1-minute resolution; polling faster only costs API calls
DEFAULT_INTERVALS = {"sqs": 60, "pubsub": 60, "pubsub_topic": 60}
