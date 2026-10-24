"""MDJaada host agent.

Threads:
  checks   every node on its own interval (default 30s, per-node `interval_s`), heartbeat
  tasks    polls the platform every few seconds for evidence / trace / test-connection work
  otlp     OTLP/HTTP JSON receiver writing spans, logs and metric points to the local store
Everything is outbound to the platform; results are buffered on disk while it is unreachable.
"""
import importlib
import logging
import os
import platform
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as wait_futures
from datetime import datetime, timezone

from . import __version__, checks, otelrules, otlp, platforms, queues, streams, tasks
from .buffer import DiskBuffer
from .client import Api
from .store import Store, now_ns

log = logging.getLogger("mdjaada.agent")


def env(name, default):
    """MDJAADA_* settings; the pre-rename FLOWTRACE_* names still work."""
    return os.environ.get(name, os.environ.get(name.replace("MDJAADA_", "FLOWTRACE_", 1), default))


class Agent:
    def __init__(self):
        self.flow_id = int(env("MDJAADA_FLOW_ID", "1"))
        self.agent_id = env("MDJAADA_AGENT_ID", platform.node() or "agent-1")
        self.interval = int(env("MDJAADA_INTERVAL", "30"))
        self.task_poll = float(env("MDJAADA_TASK_POLL", "5"))
        self.topology_every = int(env("MDJAADA_TOPOLOGY_INTERVAL", "60"))
        self.retention_hours = float(env("MDJAADA_RETENTION_HOURS", "72"))
        data_dir = env("MDJAADA_DATA_DIR", "./mdjaada-data")
        os.makedirs(data_dir, exist_ok=True)
        buf = DiskBuffer(os.path.join(data_dir, "outbox.jsonl"),
                         int(float(env("MDJAADA_BUFFER_MB", "50")) * 1024 * 1024))
        self.api = Api(env("MDJAADA_API", "http://localhost:8000"), env("MDJAADA_API_KEY", "dev-key"), buf)
        self.store = Store(env("MDJAADA_STORE", os.path.join(data_dir, "telemetry.db")))
        otelrules.store = self.store
        # OTel metric points are only needed for rules (short windows) and the metric-name list
        self.metrics_retention_hours = float(env("MDJAADA_METRICS_RETENTION_HOURS", "6"))
        self.assigned_only = env("MDJAADA_ASSIGNED_ONLY", "").lower() in ("1", "true", "yes")
        self.k8s_discovery = env("MDJAADA_K8S_DISCOVERY", "").lower() in ("1", "true", "yes")
        self.k8s_namespaces = [n.strip() for n in env("MDJAADA_K8S_NAMESPACES", "default").split(",") if n.strip()]
        self.discovery_every = int(env("MDJAADA_DISCOVERY_INTERVAL", "300"))
        self.kafka_discovery = env("MDJAADA_KAFKA_DISCOVERY", "true").lower() in ("1", "true", "yes")
        self.otlp_port = int(env("MDJAADA_OTLP_PORT", "4318"))
        self.slow_ms = float(env("MDJAADA_SLOW_SPAN_MS", "500"))
        self.sample = float(env("MDJAADA_HEALTHY_SAMPLE_RATE", "0.1"))
        self.stop = threading.Event()
        self.nodes: list[dict] = []  # last known config: keep checking through platform outages
        self.last_checked: dict[str, float] = {}
        checks.type_checks()
        self.pool = ThreadPoolExecutor(max_workers=int(env("MDJAADA_CHECK_WORKERS", "16")))
        # a check that hangs (driver, network) must not stall every other node: report it after this deadline
        self.check_deadline = float(env("MDJAADA_CHECK_DEADLINE", "25"))
        self.inflight: dict = {}  # node_id -> future of a check that is still running

    # ---------- checks ----------
    def resolve(self, node: dict) -> dict:
        """Topic/group nodes get their cluster node's connection settings."""
        if node.get("type") in streams.REFERENCES_CLUSTER:
            return streams.with_cluster(node, {n["node_id"]: n for n in self.nodes})
        return node

    def _check(self, node: dict):
        try:
            r = checks.run_check(self.resolve(node))
        except Exception as exc:  # a bug in one check must not stop the others
            r = checks.Result(False, f"check crashed: {exc}")
        if r is None:
            return None
        if FROZEN and "install " in r.message and "agent host" in r.message:
            r.message += (" (not included in the agent binary: install the Python agent with --drivers aws/gcp/azure/"
                          "snowflake, or use the Docker image)")
        return {"node_id": node["node_id"], "ok": r.ok, "warn": r.warn, "latency_ms": r.latency_ms,
                "message": r.message[:500], "metrics": r.metrics, "events": r.events,
                "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}

    def mine(self, node: dict) -> bool:
        """Nodes with an `agent` setting are checked only by that agent (e.g. one inside each cluster);
        with MDJAADA_ASSIGNED_ONLY=true this agent checks nothing else."""
        assigned = str((node.get("config") or {}).get("agent") or "").strip()
        return assigned == self.agent_id if assigned else not self.assigned_only

    def check_cycle(self, force: bool = False) -> int:
        now = time.monotonic()
        def every(n):
            return float((n.get("config") or {}).get("interval_s") or checks.DEFAULT_INTERVALS.get(n.get("type"))
                         or self.interval)

        for node_id, fut in list(self.inflight.items()):
            if fut.done():
                del self.inflight[node_id]  # a hung check finally returned; its stale result is dropped
        due = [n for n in self.nodes if self.mine(n) and n["node_id"] not in self.inflight and
               (force or now - self.last_checked.get(n["node_id"], -1e9) >= every(n) - 0.5)]
        if not due:
            return 0
        for n in due:
            self.last_checked[n["node_id"]] = now
        futures = {self.pool.submit(self._check, n): n for n in due}
        done, late = wait_futures(futures, timeout=self.check_deadline)
        results = [r for f in done if (r := f.result())]
        for f in late:
            n = futures[f]
            self.inflight[n["node_id"]] = f
            log.warning("check of %s still running after %.0fs", n.get("name"), self.check_deadline)
            results.append({"node_id": n["node_id"], "ok": False, "warn": False, "latency_ms": None,
                            "message": f"check did not finish within {self.check_deadline:g}s (no answer)",
                            "metrics": {}, "events": [],
                            "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")})
        if results:
            self.api.post_or_buffer("/agent/results",
                                    {"flow_id": self.flow_id, "agent_id": self.agent_id, "results": results})
        log.info("checked %d/%d nodes", len(results), len(due))
        return len(results)

    def refresh(self) -> None:
        try:
            self.api.call("POST", "/agent/heartbeat",
                          {"flow_id": self.flow_id, "agent_id": self.agent_id, "version": __version__,
                           "assigned_only": self.assigned_only})
            self.nodes = self.api.call("GET", f"/agent/config?flow_id={self.flow_id}") or []
            self.api.flush()
        except Exception as exc:
            log.warning("platform unreachable (%s); using last known config of %d nodes", exc, len(self.nodes))

    def check_loop(self) -> None:
        next_refresh = next_topology = next_prune = next_discovery = 0.0
        while not self.stop.is_set():
            now = time.monotonic()
            try:   # one failing step (a full disk, a bad response) must not stop the agent: try again next round
                if now >= next_refresh:
                    next_refresh = now + self.interval
                    self.refresh()
                self.check_cycle()
                if now >= next_topology:
                    next_topology = now + self.topology_every
                    self.report_topology()
                if (self.k8s_discovery or self.kafka_discovery) and now >= next_discovery:
                    next_discovery = now + self.discovery_every
                    self.discover()
                if now >= next_prune:
                    next_prune = now + 3600
                    self.store.prune(now_ns() - int(self.retention_hours * 3600e9),
                                     now_ns() - int(self.metrics_retention_hours * 3600e9))
            except Exception:
                log.exception("check loop step failed; continuing")
            self.stop.wait(min(5, self.interval))

    def discover(self) -> None:
        """Kubernetes Services and Kafka topics/consumer groups -> suggested nodes."""
        nodes = []
        if self.k8s_discovery:
            try:
                nodes += platforms.discover_kubernetes({}, self.k8s_namespaces)
            except Exception as exc:
                log.warning("kubernetes discovery failed: %s", exc)
        if self.kafka_discovery:
            finders = {"kafka_cluster": streams.discover_kafka, "rabbitmq_cluster": queues.discover_rabbitmq}
            for cluster in [n for n in self.nodes if n.get("type") in finders and self.mine(n)]:
                try:
                    nodes += finders[cluster["type"]](cluster)
                except Exception as exc:
                    log.warning("discovery for %s failed: %s", cluster.get("name"), exc)
        for n in nodes:  # whoever can see the cluster checks its nodes
            n["config"]["agent"] = self.agent_id
        log.info("discovered %d nodes", len(nodes))
        if nodes:
            self.api.post_or_buffer("/agent/discovery",
                                    {"flow_id": self.flow_id, "agent_id": self.agent_id, "nodes": nodes})

    def report_topology(self) -> None:
        try:
            pairs = tasks.topology(self.store, now_ns() - int(max(self.topology_every, 300) * 1e9))
        except Exception as exc:
            log.warning("topology query failed: %s", exc)
            return
        if pairs:
            self.api.post_or_buffer("/agent/topology",
                                    {"flow_id": self.flow_id, "agent_id": self.agent_id, "pairs": pairs})

    # ---------- platform tasks ----------
    def task_cycle(self) -> int:
        try:
            todo = self.api.call("GET", f"/agent/tasks?flow_id={self.flow_id}&agent_id={self.agent_id}") or []
        except Exception:
            return 0
        for t in todo:
            try:
                payload = t.get("payload") or {}
                if t["kind"] == "test_node":  # a topic/group test uses its cluster's connection settings
                    payload = {**payload, "config": self.resolve({"type": payload.get("type"),
                                                                  "config": payload.get("config")})["config"]}
                body = {"ok": True, "result": tasks.run(self.store, t["kind"], payload)}
            except Exception as exc:
                log.exception("task %s failed", t.get("id"))
                body = {"ok": False, "error": str(exc)[:1000]}
            self.api.post_or_buffer(f"/agent/tasks/{t['id']}/result", body)
        return len(todo)

    def task_loop(self) -> None:
        while not self.stop.is_set():
            self.task_cycle()
            self.stop.wait(self.task_poll)

    def resolve_flow(self) -> None:
        """MDJAADA_FLOW_NAME lets a deployment refer to its flow by name (ids differ per install)."""
        name = env("MDJAADA_FLOW_NAME", "")
        while name and not self.stop.is_set():
            try:
                match = [f for f in self.api.call("GET", "/flows") if f["name"] == name]
                if match:
                    self.flow_id = match[0]["id"]
                    return
                log.info("waiting for flow %r to be created", name)
            except Exception as exc:
                log.info("waiting for platform (%s)", exc)
            self.stop.wait(5)

    def run(self) -> None:
        self.resolve_flow()
        log.info("MDJaada agent %s v%s -> %s flow=%s every %ss", self.agent_id, __version__, self.api.base,
                 self.flow_id, self.interval)
        if self.otlp_port:
            otlp.serve(self.store, self.otlp_port, slow_ms=self.slow_ms, sample_rate=self.sample)
            log.info("OTLP/HTTP JSON receiver on :%d (traces, logs, metrics)", self.otlp_port)
        threading.Thread(target=self.task_loop, name="tasks", daemon=True).start()
        self.check_loop()


FROZEN = bool(getattr(sys, "frozen", False))  # running as the single-file binary
# drivers the binary bundles (module -> what needs it); cloud SDKs stay optional
BUNDLED = {"psycopg": "Postgres", "pymysql": "MySQL / MariaDB", "pymongo": "MongoDB",
           "confluent_kafka": "Kafka cluster / topic / consumer group", "yaml": "Kubernetes kubeconfig"}
OPTIONAL = {"boto3": "AWS SQS / CloudWatch", "google.cloud.monitoring_v3": "Google Pub/Sub",
            "azure.monitor.query": "Azure Monitor", "snowflake.connector": "Snowflake"}


def self_test() -> int:
    """Print what this agent can check; in the binary, fail if a bundled driver does not load."""
    libc = " ".join(platform.libc_ver()).strip()
    print(f"mdjaada-agent {__version__} ({'binary' if FROZEN else 'python'}, Python {platform.python_version()}, "
          f"{platform.machine()}{', ' + libc if libc else ''})")
    types = checks.type_checks()
    print(f"node types: {len(types)}")
    missing = []
    for mod, what in {**BUNDLED, **OPTIONAL}.items():
        try:
            m = importlib.import_module(mod)
            extra = f" (libpq: {m.pq.__impl__})" if mod == "psycopg" else ""
            print(f"  ok       {mod:<28} {what}{extra}")
        except Exception as exc:
            print(f"  missing  {mod:<28} {what} ({type(exc).__name__})")
            if mod in BUNDLED:
                missing.append(mod)
    if FROZEN and missing:
        print(f"FAIL: bundled drivers missing: {', '.join(missing)}")
        return 1
    if len(types) < 18:
        print("FAIL: node types not registered")
        return 1
    print("self-test ok")
    return 0


def main() -> None:
    if "--version" in sys.argv[1:]:
        print(f"mdjaada-agent {__version__}")
        return
    if "--self-test" in sys.argv[1:]:
        sys.exit(self_test())
    logging.basicConfig(level=env("MDJAADA_LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    agent = Agent()
    signal.signal(signal.SIGTERM, lambda *_: agent.stop.set())
    try:
        agent.run()
    except KeyboardInterrupt:
        agent.stop.set()
