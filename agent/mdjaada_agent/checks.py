"""Node library: health checks per node type (blueprint section 7).

Each node runs the generic layer first (TCP port, HTTP health URL, Prometheus metric) and then
the type-specific checks. Results combine: any failure -> not ok, any out-of-bounds value -> warn
(shown as "degraded" without counting towards "down").

Secrets never have to leave the customer network: any config value written as "env:NAME" is
read from the agent's environment, so the platform only stores the reference.
"""
import base64
import json
import os
import socket
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from . import USER_AGENT, promtext

TIMEOUT = float(os.environ.get("MDJAADA_CHECK_TIMEOUT", os.environ.get("FLOWTRACE_CHECK_TIMEOUT", "5")))
_memory: dict[str, Any] = {}  # per-node values remembered between cycles (row counts, ...)


@dataclass
class Result:
    ok: bool
    message: str
    warn: bool = False
    latency_ms: Optional[float] = None
    metrics: dict = field(default_factory=dict)
    events: list = field(default_factory=list)  # changes observed, e.g. {"kind": "deploy", "message": "rollout ..."}


def resolve(value):
    if isinstance(value, str) and value.startswith("env:"):
        return os.environ.get(value[4:], "")
    return value


def get(cfg: dict, key: str, default=None):
    v = resolve(cfg.get(key))
    return default if v in (None, "") else v


def num(cfg: dict, key: str) -> Optional[float]:
    v = get(cfg, key)
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def combine(parts: list[Result]) -> Result:
    metrics: dict = {}
    events: list = []
    for p in parts:
        metrics.update(p.metrics)
        events.extend(p.events)
    failed = [p for p in parts if not p.ok]
    warned = [p for p in parts if p.ok and p.warn]
    shown = (failed + warned) or parts  # failures first, then warnings
    return Result(
        ok=not failed,
        warn=bool(warned),
        latency_ms=next((p.latency_ms for p in parts if p.latency_ms is not None), None),
        message="; ".join(p.message for p in shown),
        metrics=metrics,
        events=events,
    )


def thresholds(value: float, cfg: dict, prefix: str, label: str, unit: str = "") -> Result:
    """<prefix>_warn / <prefix>_fail are upper bounds; <prefix>_warn_below / _fail_below lower bounds."""
    shown = f"{label} {round(value, 1):g}{unit}"
    fail, warn = num(cfg, f"{prefix}_fail"), num(cfg, f"{prefix}_warn")
    fail_below, warn_below = num(cfg, f"{prefix}_fail_below"), num(cfg, f"{prefix}_warn_below")
    if fail is not None and value > fail:
        return Result(False, f"{shown} > {fail:g}{unit}")
    if fail_below is not None and value < fail_below:
        return Result(False, f"{shown} < {fail_below:g}{unit}")
    if warn is not None and value > warn:
        return Result(True, f"{shown} > {warn:g}{unit}", warn=True)
    if warn_below is not None and value < warn_below:
        return Result(True, f"{shown} < {warn_below:g}{unit}", warn=True)
    return Result(True, shown)


# ---------- generic layer ----------
def check_tcp(host: str, port) -> Result:
    start = time.monotonic()
    try:
        with socket.create_connection((host, int(port)), timeout=TIMEOUT):
            pass
        return Result(True, f"{host}:{port} reachable", latency_ms=(time.monotonic() - start) * 1000)
    except Exception as exc:
        return Result(False, f"tcp {host}:{port} failed: {exc}")


def http_get(url: str, cfg: Optional[dict] = None, data: Optional[bytes] = None, method: Optional[str] = None):
    cfg = cfg or {}
    headers = {"Accept": "application/json, text/plain, */*", "User-Agent": USER_AGENT}
    if data is not None:
        headers["Content-Type"] = "application/json"
    token, user, api_key = get(cfg, "token"), get(cfg, "username"), get(cfg, "api_key")
    if api_key:  # Elasticsearch / OpenSearch API keys
        headers["Authorization"] = f"ApiKey {api_key}"
    elif token:
        headers["Authorization"] = f"Bearer {token}"
    elif user:
        raw = f"{user}:{get(cfg, 'password', '')}".encode()
        headers["Authorization"] = "Basic " + base64.b64encode(raw).decode()
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    start = time.monotonic()
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        body = resp.read()
        return resp.status, body, (time.monotonic() - start) * 1000


def _json_path(doc, path: str):
    for part in path.split("."):
        doc = doc[int(part)] if isinstance(doc, list) else doc[part]
    return doc


def check_http(url: str, cfg: dict) -> Result:
    try:
        status, body, ms = http_get(url, cfg)
    except urllib.error.HTTPError as exc:
        return Result(False, f"HTTP {exc.code} from {url}", metrics={"http_status": exc.code})
    except Exception as exc:
        return Result(False, f"http {url} failed: {exc}")
    expect = get(cfg, "expect_status")
    if expect and str(status) != str(expect):
        return Result(False, f"HTTP {status}, expected {expect}", latency_ms=ms)
    field_path, want = get(cfg, "json_field"), get(cfg, "json_equals")
    if field_path:
        try:
            got = _json_path(json.loads(body), field_path)
        except Exception:
            return Result(False, f"HTTP {status} but {field_path} missing in body", latency_ms=ms)
        if want is not None and str(got) != str(want):
            return Result(False, f"{field_path}={got}, expected {want}", latency_ms=ms)
    r = thresholds(ms, cfg, "latency_ms", "latency", " ms")
    r.message = f"HTTP {status}, {r.message}"
    r.latency_ms, r.metrics = ms, {"latency_ms": round(ms, 1)}
    return r


def scrape(url: str, cfg: dict):
    _, body, _ = http_get(url, cfg)
    return promtext.parse(body.decode("utf-8", "replace"))


def check_metric(samples, selector: str, cfg: dict, prefix: str = "value", label: Optional[str] = None,
                 extra_labels: Optional[dict] = None, missing_ok: bool = False) -> Optional[Result]:
    name, labels = promtext.parse_selector(selector)
    labels.update(extra_labels or {})
    values = promtext.select(samples, name, labels)
    if not values:
        return None if missing_ok else Result(True, f"metric {selector} not found", warn=True)
    value = promtext.aggregate(values, get(cfg, "aggregate", "max"))
    r = thresholds(value, cfg, prefix, label or name)
    r.metrics = {label or name: value}
    return r


def generic(cfg: dict, default_port: Optional[int] = None) -> list[Result]:
    parts = []
    host, port = get(cfg, "host"), get(cfg, "port", default_port)
    if host and port:
        parts.append(check_tcp(host, port))
    if get(cfg, "health_url"):
        parts.append(check_http(get(cfg, "health_url"), cfg))
    if get(cfg, "metrics_url") and get(cfg, "metric"):
        try:
            r = check_metric(scrape(get(cfg, "metrics_url"), cfg), get(cfg, "metric"), cfg)
            parts.append(r)
        except Exception as exc:
            parts.append(Result(False, f"metrics scrape failed: {exc}"))
    return parts


# ---------- type-specific layer ----------
def kafka(node_id: str, cfg: dict) -> list[Result]:
    parts = []
    brokers = [b.strip() for b in str(get(cfg, "bootstrap", "")).split(",") if b.strip()]
    if brokers:
        down = []
        for b in brokers:
            host, _, port = b.rpartition(":")
            if not check_tcp(host or b, port or 9092).ok:
                down.append(b)
        if len(down) == len(brokers):
            parts.append(Result(False, f"all {len(brokers)} brokers unreachable"))
        elif down:
            parts.append(Result(True, f"{len(down)}/{len(brokers)} brokers unreachable: {', '.join(down)}", warn=True))
        else:
            parts.append(Result(True, f"{len(brokers)} brokers reachable"))
        parts[-1].metrics["brokers_down"] = len(down)
    url = get(cfg, "metrics_url")
    if url:  # kafka-exporter / kminion style metrics
        try:
            samples = scrape(url, cfg)
            labels = {}
            if get(cfg, "consumer_group"):
                labels["consumergroup"] = get(cfg, "consumer_group")
            if get(cfg, "topic"):
                labels["topic"] = get(cfg, "topic")
            lag_cfg = {**cfg, "aggregate": get(cfg, "aggregate", "sum")}
            lag = check_metric(samples, get(cfg, "lag_metric", "kafka_consumergroup_lag"), lag_cfg, "lag",
                               "consumer_lag", labels)
            parts.append(lag)
            urp = check_metric(samples, "kafka_topic_partition_under_replicated_partition", {"aggregate": "sum",
                               "urp_warn": 0}, "urp", "under_replicated_partitions",
                               {"topic": labels["topic"]} if "topic" in labels else {}, missing_ok=True)
            if urp:
                parts.append(urp)
        except Exception as exc:
            parts.append(Result(False, f"kafka metrics scrape failed: {exc}"))
    return parts


def _sql_connect(dsn: str):
    try:
        import psycopg  # type: ignore
        return psycopg.connect(dsn, connect_timeout=int(TIMEOUT), autocommit=True)
    except ImportError:
        import psycopg2  # type: ignore
        conn = psycopg2.connect(dsn, connect_timeout=int(TIMEOUT))
        conn.autocommit = True
        return conn


def _scalar(conn, sql: str):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def _age_seconds(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)  # already an age in seconds
        except ValueError:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - value).total_seconds()


def truthy(value) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def err(exc: Exception) -> str:
    text = str(exc).strip()
    return (text.splitlines()[0] if text else type(exc).__name__)[:200]


def sql_checks(node_id: str, cfg: dict, scalar: Callable[[str], Any], ping_sql: str = "select 1") -> list[Result]:
    """Checks every SQL store shares: query latency, data freshness, row-count drop, custom value.
    `freshness_query` may return a timestamp or an age in seconds."""
    start = time.monotonic()
    scalar(ping_sql)
    ms = (time.monotonic() - start) * 1000
    r = thresholds(ms, cfg, "latency_ms", "query latency", " ms")
    r.latency_ms, r.metrics = ms, {"query_latency_ms": round(ms, 1)}
    parts = [r]
    if get(cfg, "freshness_query"):
        age = _age_seconds(scalar(get(cfg, "freshness_query")))
        if age is None:
            parts.append(Result(False, "freshness query returned no rows"))
        else:
            r = thresholds(age, cfg, "freshness_s", "data age", " s")
            r.metrics = {"data_age_s": round(age)}
            parts.append(r)
    if get(cfg, "rowcount_query"):
        parts.append(rowcount(node_id, cfg, float(scalar(get(cfg, "rowcount_query")) or 0)))
    if get(cfg, "sql"):
        parts.append(sql_value(cfg, scalar(get(cfg, "sql"))))
    return parts


def rowcount(node_id: str, cfg: dict, count: float) -> Result:
    prev = _memory.get(f"{node_id}:rows")
    _memory[f"{node_id}:rows"] = count
    drop = num(cfg, "rowcount_drop_pct") or 50
    if prev and count < prev * (1 - drop / 100):
        return Result(True, f"row count fell {prev:g} -> {count:g}", warn=True, metrics={"row_count": count})
    return Result(True, f"rows {count:g}", metrics={"row_count": count})


def sql_value(cfg: dict, value) -> Result:
    value = float(value or 0)
    label = get(cfg, "value_label", "value")
    r = thresholds(value, cfg, "value", label)
    r.metrics = {label: value}
    return r


def with_connection(connect: Callable[[], Any], driver_hint: str,
                    run: Callable[[Any], list[Result]]) -> list[Result]:
    """Open a driver connection, run checks, always close; missing driver -> a clear warning."""
    try:
        conn = connect()
    except ImportError:
        return [Result(True, f"install {driver_hint} on the agent host to enable these checks", warn=True)]
    except Exception as exc:
        return [Result(False, f"connect failed: {err(exc)}")]
    try:
        return run(conn)
    except Exception as exc:
        return [Result(False, f"query failed: {err(exc)}")]
    finally:
        try:
            conn.close()
        except Exception:
            pass


LOCK_WAITS_SQL = ("select count(*) || ',' || coalesce(max(extract(epoch from now() - query_start)), 0) "
                  "from pg_stat_activity where wait_event_type = 'Lock'")


def pg_lock_waits(cfg: dict, scalar) -> list[Result]:
    """Queries stuck behind a lock: the database answers `select 1` but the app's queries hang (a long migration,
    a forgotten transaction). Defaults: warn after 10 s, fail after 60 s; seeing other users' sessions needs
    the pg_monitor role."""
    try:
        count, longest = (float(x) for x in str(scalar(LOCK_WAITS_SQL)).split(","))
    except Exception:
        return []   # older server or no permission: not a health signal
    if not count:
        return []
    limits = {"lock_wait_s_warn": get(cfg, "lock_wait_s_warn", "10"), "lock_wait_s_fail": get(cfg, "lock_wait_s_fail", "60")}
    r = thresholds(longest, limits, "lock_wait_s", f"{count:g} queries waiting on locks, longest", " s")
    r.metrics = {"lock_waiting_queries": count, "longest_lock_wait_s": round(longest, 1)}
    return [r]


def postgres(node_id: str, cfg: dict) -> list[Result]:
    dsn = get(cfg, "dsn")
    if not dsn:
        return []

    def run(conn):
        parts = sql_checks(node_id, cfg, lambda q: _scalar(conn, q))
        parts += pg_lock_waits(cfg, lambda q: _scalar(conn, q))
        if truthy(get(cfg, "replica")):
            lag = float(_scalar(conn, "select coalesce(extract(epoch from now() - pg_last_xact_replay_timestamp()), 0)") or 0)
            r = thresholds(lag, cfg, "replication_lag_s", "replication lag", " s")
            r.metrics = {"replication_lag_s": lag}
            parts.append(r)
        return parts

    return with_connection(lambda: _sql_connect(dsn), "psycopg", run)


def proxy(node_id: str, cfg: dict) -> list[Result]:
    return []  # TCP + health URL + metric (connections, 5xx rate) come from the generic layer


_airflow_cache: dict[Any, Any] = {}  # base URL -> API version; (base, user) -> (token, valid until)


def _airflow_version(cfg: dict, base: str) -> str:
    """Airflow 3 serves /api/v2 (and /api/v2/version without login); Airflow 2 serves /api/v1."""
    chosen = get(cfg, "api_version")
    if chosen in ("v1", "v2"):
        return chosen
    if base not in _airflow_cache:
        try:
            http_get(f"{base}/api/v2/version", {})
            _airflow_cache[base] = "v2"
        except urllib.error.HTTPError as exc:
            _airflow_cache[base] = "v2" if exc.code in (401, 403) else "v1"
        except Exception:
            return "v1"  # unreachable now: don't remember a guess
    return _airflow_cache[base]


def _airflow_token(cfg: dict, base: str, fresh: bool = False) -> str:
    """Airflow 3 (simple or FAB auth manager): exchange username/password for a JWT at /auth/token."""
    key = (base, get(cfg, "username"))
    cached = _airflow_cache.get(key)
    if cached and not fresh and cached[1] > time.monotonic():
        return cached[0]
    body = json.dumps({"username": get(cfg, "username"), "password": get(cfg, "password", "")}).encode()
    _, raw, _ = http_get(f"{base}/auth/token", {}, data=body, method="POST")
    token = json.loads(raw)["access_token"]
    _airflow_cache[key] = (token, time.monotonic() + 600)  # refreshed early, and again on a 401
    return token


def _airflow_get(cfg: dict, base: str, version: str, path: str):
    url = f"{base}/api/{version}{path}"
    if version == "v2" and get(cfg, "username") and not get(cfg, "token"):
        try:
            return http_get(url, {"token": _airflow_token(cfg, base)})
        except urllib.error.HTTPError as exc:
            if exc.code != 401:
                raise
            return http_get(url, {"token": _airflow_token(cfg, base, fresh=True)})  # token expired
    return http_get(url, cfg)  # Airflow 2 basic auth, or a static token


def airflow(node_id: str, cfg: dict) -> list[Result]:
    base, dag = get(cfg, "base_url"), get(cfg, "dag_id")
    if not (base and dag):
        return []
    base = base.rstrip("/")
    version = _airflow_version(cfg, base)
    order = "-logical_date" if version == "v2" else "-execution_date"
    path = f"/dags/{dag}/dagRuns?order_by={order}&limit=1"
    try:
        _, body, ms = _airflow_get(cfg, base, version, path)
        runs = json.loads(body).get("dag_runs", [])
    except urllib.error.HTTPError as exc:
        hint = {401: " (check username/password)", 403: " (user lacks permission to read DAG runs)",
                404: f" (no DAG {dag}?)"}.get(exc.code, "")
        return [Result(False, f"airflow API: HTTP {exc.code}{hint}")]
    except Exception as exc:
        return [Result(False, f"airflow API failed: {err(exc)}")]
    if not runs:
        try:  # Airflow lists no runs for a DAG that doesn't exist either: tell a typo from a new DAG
            _airflow_get(cfg, base, version, f"/dags/{dag}")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return [Result(False, f"DAG {dag} not found in Airflow (check the DAG id)", latency_ms=ms)]
        except Exception:
            pass
        return [Result(True, f"DAG {dag} has no runs", warn=True, latency_ms=ms)]
    run = runs[0]
    state = run.get("state")
    started = run.get("start_date")
    parts = []
    if state == "failed":
        parts.append(Result(False, f"DAG {dag} run {run.get('dag_run_id')} failed", latency_ms=ms))
    elif state == "running" and started:
        r = thresholds(_age_seconds(started), cfg, "duration_s", f"DAG {dag} running for", " s")
        r.latency_ms = ms
        parts.append(r)
    else:
        parts.append(Result(True, f"DAG {dag} last run {state}", latency_ms=ms))
    parts[-1].metrics["last_run_state"] = state
    parts[-1].metrics["airflow_api"] = version
    if num(cfg, "sla_s"):  # data freshness: last *successful* run must be recent enough
        try:
            _, body, _ = _airflow_get(cfg, base, version, f"{path}&state=success")
            ok_runs = json.loads(body).get("dag_runs", [])
            end = ok_runs[0].get("end_date") if ok_runs else None
            age = _age_seconds(end) if end else None
            if age is None or age > num(cfg, "sla_s"):
                parts.append(Result(False, f"SLA miss: no successful run in {num(cfg, 'sla_s'):g} s"))
            else:
                parts.append(Result(True, f"last success {age:.0f} s ago", metrics={"since_success_s": round(age)}))
        except Exception as exc:
            parts.append(Result(True, f"could not read last success: {err(exc)}", warn=True))
    return parts


def mlflow(node_id: str, cfg: dict) -> list[Result]:
    base, exp = get(cfg, "tracking_url"), get(cfg, "experiment_id")
    if not (base and exp):
        return []
    body = {"experiment_ids": [str(exp)], "max_results": 10, "order_by": ["attributes.start_time DESC"]}
    try:
        _, raw, ms = http_get(f"{base.rstrip('/')}/api/2.0/mlflow/runs/search", cfg,
                              data=json.dumps(body).encode(), method="POST")
        runs = json.loads(raw).get("runs", [])
    except Exception as exc:
        return [Result(False, f"mlflow API failed: {exc}")]
    if not runs:
        return [Result(True, "no runs in experiment", warn=True, latency_ms=ms)]
    latest = runs[0]["info"]
    parts = []
    if latest.get("status") == "FAILED":
        parts.append(Result(False, f"latest run {latest.get('run_id', '')[:8]} FAILED", latency_ms=ms))
    else:
        parts.append(Result(True, f"latest run {latest.get('status')}", latency_ms=ms))
    metric = get(cfg, "metric")
    if metric:
        finished = [r for r in runs if r["info"].get("status") == "FINISHED"]
        vals = []
        for r in finished[:2]:
            m = {x["key"]: x["value"] for x in r.get("data", {}).get("metrics", [])}
            if metric in m:
                vals.append(float(m[metric]))
        if len(vals) == 2:
            cur, prev = vals
            higher_better = str(get(cfg, "higher_is_better", "true")).lower() != "false"
            change = (cur - prev) / abs(prev) * 100 if prev else 0.0
            worse = -change if higher_better else change
            limit = num(cfg, "regression_pct") or 5
            msg = f"{metric} {prev:g} -> {cur:g} ({change:+.1f}%)"
            parts.append(Result(True, msg, warn=worse > limit, metrics={metric: cur}))
        elif vals:
            parts.append(Result(True, f"{metric} {vals[0]:g}", metrics={metric: vals[0]}))
    return parts


def custom(node_id: str, cfg: dict) -> list[Result]:
    if get(cfg, "sql") and get(cfg, "dsn") and not get(cfg, "freshness_query"):
        return with_connection(lambda: _sql_connect(get(cfg, "dsn")), "psycopg",
                               lambda conn: [sql_value(cfg, _scalar(conn, get(cfg, "sql")))])
    return []


TYPE_CHECKS: dict[str, Callable[[str, dict], list[Result]]] = {
    "kafka": kafka, "postgres": postgres, "proxy": proxy, "airflow": airflow, "mlflow": mlflow, "custom": custom,
}
DEFAULT_PORTS = {"postgres": 5432, "kafka": 9092}

DEFAULT_INTERVALS: dict[str, int] = {}
OWN_CONNECTION: set[str] = set()
_loaded = False


def type_checks() -> dict[str, Callable[[str, dict], list[Result]]]:
    """Register the store and queue checks on first use (they import the helpers above, so importing
    them here at module load would be circular)."""
    global _loaded
    if not _loaded:
        from . import platforms, queues, stores, streams
        for mod in (stores, queues, platforms, streams):
            TYPE_CHECKS.update(mod.TYPES)
            DEFAULT_INTERVALS.update(getattr(mod, "DEFAULT_INTERVALS", {}))
            OWN_CONNECTION.update(getattr(mod, "OWN_CONNECTION", set()))
        _loaded = True
    return TYPE_CHECKS


def run_check(node: dict) -> Optional[Result]:
    cfg = node.get("config") or {}
    node_type = node.get("type", "")
    extra = type_checks().get(node_type)
    # stores that open their own connection would only duplicate the generic TCP check
    generic_cfg = {k: v for k, v in cfg.items() if k not in ("host", "port")} if node_type in OWN_CONNECTION else cfg
    parts = generic(generic_cfg, DEFAULT_PORTS.get(node_type) if get(cfg, "host") else None)
    if extra:
        parts += extra(node.get("node_id", ""), cfg)
    from . import otelrules  # lazy: otelrules imports this module
    rule = otelrules.check(cfg)
    if rule:
        parts.append(rule)
    if not parts:
        return None  # nothing configured yet
    return combine(parts)
