"""Node library, part 2: databases and data stores.

| type          | connection                  | driver (optional, on the agent host) |
|---------------|-----------------------------|--------------------------------------|
| redis         | RESP over TCP               | none (built in)                      |
| elasticsearch | REST (also OpenSearch)      | none                                 |
| clickhouse    | HTTP interface (:8123)      | none                                 |
| mysql         | MySQL protocol              | PyMySQL                              |
| mongodb       | MongoDB wire protocol       | pymongo                              |
| snowflake     | Snowflake connector         | snowflake-connector-python           |

Every store reports a latency first and then store-specific health. A missing driver produces a
"degraded: install X" result rather than a false outage.
"""
import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

from . import USER_AGENT
from .checks import (TIMEOUT, Result, _age_seconds, _memory, err, get, http_get, num, rowcount, sql_checks,
                     thresholds, truthy, with_connection)


def defaults(cfg: dict, **values) -> dict:
    """cfg with built-in thresholds filled in where the user left the setting empty."""
    out = dict(cfg)
    for k, v in values.items():
        if get(cfg, k) is None:
            out[k] = v
    return out


def _pct(used: float, total: float) -> Optional[float]:
    return round(used / total * 100, 1) if total else None


# ---------------------------------------------------------------- Redis
class Resp:
    """Minimal Redis protocol client: enough for AUTH, PING and INFO."""

    def __init__(self, host: str, port: int, timeout: float = TIMEOUT):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.buf = self.sock.makefile("rb")

    def command(self, *args: str):
        out = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.sock.sendall(b"".join(out))
        return self._reply()

    def _reply(self):
        line = self.buf.readline()
        if not line:
            raise ConnectionError("connection closed by server")
        kind, rest = line[:1], line[1:-2]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.buf.read(n + 2)[:-2]
            return data.decode("utf-8", "replace")
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self._reply() for _ in range(n)]
        raise RuntimeError(f"unexpected reply {line[:40]!r}")

    def close(self):
        try:
            self.buf.close()
        finally:
            self.sock.close()


def parse_info(text: str) -> dict[str, str]:
    info = {}
    for line in text.splitlines():
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            info[k.strip()] = v.strip()
    return info


def redis(node_id: str, cfg: dict) -> list[Result]:
    host = get(cfg, "host")
    if not host:
        return []
    port = int(get(cfg, "port", 6379))

    def connect():
        c = Resp(host, port)
        password = get(cfg, "password")
        if password:
            user = get(cfg, "username")
            c.command(*(["AUTH", user, password] if user else ["AUTH", password]))
        return c

    def run(c: Resp) -> list[Result]:
        start = time.monotonic()
        c.command("PING")
        ms = (time.monotonic() - start) * 1000
        r = thresholds(ms, cfg, "latency_ms", "ping", " ms")
        r.latency_ms, r.metrics = ms, {"ping_ms": round(ms, 1)}
        parts = [r]
        info = parse_info(c.command("INFO") or "")

        used, limit = float(info.get("used_memory", 0)), float(info.get("maxmemory", 0))
        pct = _pct(used, limit)
        if pct is not None:
            r = thresholds(pct, defaults(cfg, memory_pct_warn=90), "memory_pct", "memory used", "%")
            r.metrics = {"memory_used_pct": pct}
            parts.append(r)
        else:
            parts.append(Result(True, f"memory {used / 1048576:.0f} MB (no maxmemory)",
                                metrics={"memory_used_mb": round(used / 1048576, 1)}))

        clients = int(info.get("connected_clients", 0))
        r = thresholds(clients, cfg, "clients", "clients")
        r.metrics = {"connected_clients": clients, "blocked_clients": int(info.get("blocked_clients", 0))}
        parts.append(r)

        evicted = int(info.get("evicted_keys", 0))
        prev = _memory.get(f"{node_id}:evicted")
        _memory[f"{node_id}:evicted"] = evicted
        if prev is not None and evicted > prev:
            new = evicted - prev
            parts.append(Result(True, f"{new} keys evicted since last check", warn=new > (num(cfg, "evictions_warn") or 0),
                                metrics={"evicted_since_last": new}))

        if info.get("role") == "slave":
            link = info.get("master_link_status", "down")
            if link != "up":
                parts.append(Result(False, f"replica link to primary is {link}"))
            else:
                lag = float(info.get("master_last_io_seconds_ago", 0))
                r = thresholds(lag, cfg, "replication_lag_s", "replica last IO", " s")
                r.metrics = {"replica_io_ago_s": lag}
                parts.append(r)
        if info.get("rdb_last_bgsave_status", "ok") != "ok":
            parts.append(Result(True, "last RDB snapshot failed", warn=True))
        if info.get("aof_enabled") == "1" and info.get("aof_last_write_status", "ok") != "ok":
            parts.append(Result(False, "AOF writes failing"))
        return parts

    return with_connection(connect, "nothing (built in)", run)


# ---------------------------------------------------------------- Elasticsearch / OpenSearch
def _es(cfg: dict, path: str, body: Optional[dict] = None):
    base = get(cfg, "url", "").rstrip("/")
    data = json.dumps(body).encode() if body is not None else None
    status, raw, ms = http_get(base + path, cfg, data=data, method="POST" if body is not None else "GET")
    return json.loads(raw), ms


def elasticsearch(node_id: str, cfg: dict) -> list[Result]:
    if not get(cfg, "url"):
        return []
    try:
        health, ms = _es(cfg, "/_cluster/health")
    except Exception as exc:
        return [Result(False, f"cluster health failed: {err(exc)}")]
    status = health.get("status", "unknown")
    unassigned = int(health.get("unassigned_shards", 0))
    nodes = int(health.get("number_of_nodes", 0))
    metrics = {"cluster_status": status, "nodes": nodes, "unassigned_shards": unassigned}
    msg = f"cluster {status}, {nodes} nodes, {unassigned} unassigned shards"
    r = Result(status != "red", msg, warn=status == "yellow" and not truthy(get(cfg, "allow_yellow")),
               latency_ms=ms, metrics=metrics)
    r2 = thresholds(ms, cfg, "latency_ms", "health latency", " ms")
    parts = [r] + ([r2] if not r2.ok or r2.warn else [])
    expected = num(cfg, "expected_nodes")
    if expected and nodes < expected:
        parts.append(Result(False, f"only {nodes} of {expected:g} nodes in cluster"))

    try:
        stats, _ = _es(cfg, "/_nodes/stats/jvm")
        heap = max((n["jvm"]["mem"]["heap_used_percent"] for n in stats.get("nodes", {}).values()), default=None)
        if heap is not None:
            r = thresholds(float(heap), defaults(cfg, heap_pct_warn=85), "heap_pct", "max JVM heap", "%")
            r.metrics = {"max_heap_used_pct": heap}
            parts.append(r)
    except Exception:
        pass  # restricted users may not read node stats

    index = get(cfg, "index")
    if index:
        field = get(cfg, "timestamp_field", "@timestamp")
        try:
            res, _ = _es(cfg, f"/{urllib.parse.quote(index, safe='*,-_')}/_search",
                         {"size": 0, "aggs": {"latest": {"max": {"field": field}}}})
            latest = (res.get("aggregations") or {}).get("latest", {}).get("value")
            if latest is None:
                parts.append(Result(False, f"index {index} has no documents with {field}"))
            else:
                age = time.time() - float(latest) / 1000
                r = thresholds(age, cfg, "freshness_s", f"{index} newest doc age", " s")
                r.metrics = {"data_age_s": round(age)}
                parts.append(r)
            if truthy(get(cfg, "track_count")):
                cnt, _ = _es(cfg, f"/{urllib.parse.quote(index, safe='*,-_')}/_count")
                parts.append(rowcount(node_id, cfg, float(cnt.get("count", 0))))
        except Exception as exc:
            parts.append(Result(False, f"index {index} query failed: {err(exc)}"))
    return parts


# ---------------------------------------------------------------- ClickHouse
def clickhouse_query(cfg: dict, sql: str):
    """One value via the HTTP interface (TabSeparated); numbers come back as floats."""
    base = get(cfg, "url", "").rstrip("/")
    params = {"database": get(cfg, "database", "default")}
    url = f"{base}/?{urllib.parse.urlencode(params)}"
    headers = {"X-ClickHouse-User": get(cfg, "username", "default"), "User-Agent": USER_AGENT}
    if get(cfg, "password"):
        headers["X-ClickHouse-Key"] = get(cfg, "password")
    req = urllib.request.Request(url, data=f"{sql} FORMAT TabSeparated".encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            line = resp.read().decode().split("\n", 1)[0]
    except urllib.error.HTTPError as exc:  # ClickHouse puts the real error in the body
        raise RuntimeError(exc.read().decode("utf-8", "replace").strip()[:200] or f"HTTP {exc.code}") from None
    first = line.split("\t", 1)[0] if line else None
    if first in (None, "", "\\N"):
        return None
    try:
        return float(first)
    except ValueError:
        return first


class _NoConn:
    def close(self):
        pass


def clickhouse(node_id: str, cfg: dict) -> list[Result]:
    if not get(cfg, "url"):
        return []

    def run(_):
        parts = sql_checks(node_id, cfg, lambda q: clickhouse_query(cfg, q.rstrip(";")), "SELECT 1")
        replicas = clickhouse_query(cfg, "SELECT count() FROM system.replicas")
        if replicas:
            ro = clickhouse_query(cfg, "SELECT countIf(is_readonly) FROM system.replicas") or 0
            delay = clickhouse_query(cfg, "SELECT max(absolute_delay) FROM system.replicas") or 0
            if ro:
                parts.append(Result(False, f"{ro:g} replicated tables are read-only (Keeper/ZooKeeper lost?)"))
            r = thresholds(float(delay), cfg, "replication_lag_s", "replica delay", " s")
            r.metrics = {"replica_delay_s": delay}
            parts.append(r)
        max_parts = clickhouse_query(cfg, "SELECT max(c) FROM (SELECT count() AS c FROM system.parts "
                                          "WHERE active GROUP BY database, table, partition)") or 0
        r = thresholds(float(max_parts), defaults(cfg, parts_warn=300), "parts", "max parts per partition")
        r.metrics = {"max_parts_per_partition": max_parts}
        parts.append(r)
        stuck = clickhouse_query(cfg, "SELECT count() FROM system.mutations WHERE NOT is_done AND latest_fail_reason != ''") or 0
        if stuck:
            parts.append(Result(True, f"{stuck:g} mutations failing", warn=True, metrics={"failing_mutations": stuck}))
        return parts

    return with_connection(_NoConn, "nothing (built in)", run)


# ---------------------------------------------------------------- MySQL / MariaDB
def _mysql_connect(cfg: dict):
    import pymysql  # type: ignore
    return pymysql.connect(host=get(cfg, "host"), port=int(get(cfg, "port", 3306)), user=get(cfg, "username", "root"),
                           password=get(cfg, "password", ""), database=get(cfg, "database") or None,
                           connect_timeout=int(TIMEOUT), read_timeout=int(TIMEOUT), autocommit=True)


def _rows(conn, sql: str) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description or []]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()


def _first(conn, sql: str):
    rows = _rows(conn, sql)
    return next(iter(rows[0].values())) if rows else None


def mysql(node_id: str, cfg: dict) -> list[Result]:
    if not get(cfg, "host"):
        return []

    def run(conn):
        parts = sql_checks(node_id, cfg, lambda q: _first(conn, q))
        status = {r["Variable_name"]: r["Value"] for r in _rows(conn, "SHOW GLOBAL STATUS WHERE Variable_name IN "
                                                                     "('Threads_connected','Threads_running')")}
        conns, limit = float(status.get("Threads_connected", 0)), float(_first(conn, "SELECT @@max_connections") or 0)
        pct = _pct(conns, limit)
        r = thresholds(pct or 0, defaults(cfg, connections_pct_warn=85), "connections_pct", "connections used", "%")
        r.metrics = {"connections": conns, "connections_used_pct": pct, "threads_running": float(status.get("Threads_running", 0))}
        parts.append(r)
        if truthy(get(cfg, "replica")):
            try:
                rows = _rows(conn, "SHOW REPLICA STATUS")
            except Exception:
                rows = _rows(conn, "SHOW SLAVE STATUS")  # MySQL < 8.0.22, MariaDB
            if not rows:
                parts.append(Result(False, "replica=true but this server is not replicating"))
            else:
                rs = rows[0]
                io = rs.get("Replica_IO_Running", rs.get("Slave_IO_Running"))
                sql_thread = rs.get("Replica_SQL_Running", rs.get("Slave_SQL_Running"))
                lag = rs.get("Seconds_Behind_Source", rs.get("Seconds_Behind_Master"))
                if io != "Yes" or sql_thread != "Yes":
                    last = rs.get("Last_Error") or rs.get("Last_IO_Error") or rs.get("Last_SQL_Error") or ""
                    parts.append(Result(False, f"replication stopped (IO={io}, SQL={sql_thread}) {last}".strip()))
                else:
                    r = thresholds(float(lag or 0), cfg, "replication_lag_s", "replication lag", " s")
                    r.metrics = {"replication_lag_s": float(lag or 0)}
                    parts.append(r)
        return parts

    return with_connection(lambda: _mysql_connect(cfg), "PyMySQL (pip install pymysql)", run)


# ---------------------------------------------------------------- MongoDB
def _mongo_connect(cfg: dict):
    from pymongo import MongoClient  # type: ignore
    return MongoClient(get(cfg, "uri"), serverSelectionTimeoutMS=int(TIMEOUT * 1000),
                       connectTimeoutMS=int(TIMEOUT * 1000))


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def mongodb(node_id: str, cfg: dict) -> list[Result]:
    if not get(cfg, "uri"):
        return []

    def run(client):
        start = time.monotonic()
        client.admin.command("ping")
        ms = (time.monotonic() - start) * 1000
        r = thresholds(ms, cfg, "latency_ms", "ping", " ms")
        r.latency_ms, r.metrics = ms, {"ping_ms": round(ms, 1)}
        parts = [r]

        conns = client.admin.command("serverStatus").get("connections", {})
        cur, avail = float(conns.get("current", 0)), float(conns.get("available", 0))
        pct = _pct(cur, cur + avail)
        r = thresholds(pct or 0, defaults(cfg, connections_pct_warn=85), "connections_pct", "connections used", "%")
        r.metrics = {"connections": cur, "connections_used_pct": pct}
        parts.append(r)

        try:
            rs = client.admin.command("replSetGetStatus")
        except Exception as exc:  # standalone server: code 76 NoReplicationEnabled
            if getattr(exc, "code", None) not in (76, 94) and "not running with --replSet" not in str(exc):
                raise
            rs = None
        if rs:
            members = rs.get("members", [])
            primary = next((m for m in members if m.get("stateStr") == "PRIMARY"), None)
            unhealthy = [m.get("name") for m in members if not m.get("health")]
            if not primary:
                parts.append(Result(False, f"replica set {rs.get('set')} has no PRIMARY"))
            else:
                lag = max(((_utc(primary["optimeDate"]) - _utc(m["optimeDate"])).total_seconds()
                           for m in members if m.get("stateStr") == "SECONDARY" and m.get("optimeDate")), default=0.0)
                r = thresholds(lag, cfg, "replication_lag_s", "max secondary lag", " s")
                r.metrics = {"replication_lag_s": lag, "members": len(members)}
                parts.append(r)
            if unhealthy:
                parts.append(Result(True, f"unhealthy members: {', '.join(unhealthy)}", warn=True))

        coll_name = get(cfg, "collection")
        if coll_name:
            coll = client[get(cfg, "database", "test")][coll_name]
            field = get(cfg, "freshness_field")
            if field:
                doc = coll.find_one({field: {"$exists": True}}, sort=[(field, -1)], projection={field: 1})
                if not doc:
                    parts.append(Result(False, f"{coll_name} has no documents with {field}"))
                else:
                    value = doc[field]
                    age = (datetime.now(timezone.utc) - _utc(value)).total_seconds() if isinstance(value, datetime) \
                        else _age_seconds(value)
                    r = thresholds(age, cfg, "freshness_s", f"{coll_name} newest doc age", " s")
                    r.metrics = {"data_age_s": round(age)}
                    parts.append(r)
            if truthy(get(cfg, "track_count")):
                parts.append(rowcount(node_id, cfg, float(coll.estimated_document_count())))
        return parts

    return with_connection(lambda: _mongo_connect(cfg), "pymongo (pip install pymongo)", run)


# ---------------------------------------------------------------- Snowflake
def _snowflake_connect(cfg: dict):
    import snowflake.connector  # type: ignore
    args = {k: get(cfg, k) for k in ("account", "user", "password", "warehouse", "database", "schema", "role",
                                     "authenticator") if get(cfg, k)}
    return snowflake.connector.connect(login_timeout=int(TIMEOUT * 3), network_timeout=int(TIMEOUT * 6), **args)


def snowflake(node_id: str, cfg: dict) -> list[Result]:
    """Every query that touches tables resumes the warehouse and uses credits: the agent checks
    Snowflake nodes every 5 minutes unless interval_s says otherwise."""
    if not (get(cfg, "account") and get(cfg, "user")):
        return []

    def run(conn):
        def scalar(q):
            cur = conn.cursor()
            try:
                cur.execute(q)
                row = cur.fetchone()
                return row[0] if row else None
            finally:
                cur.close()

        parts = sql_checks(node_id, cfg, scalar, "SELECT 1")
        if truthy(get(cfg, "check_tasks")):
            failed = scalar("SELECT count(*) FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY("
                            "SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()))) "
                            "WHERE STATE = 'FAILED'") or 0
            parts.append(Result(not failed, f"{failed:g} failed tasks in the last hour" if failed else "no failed tasks in the last hour",
                                metrics={"failed_tasks_1h": float(failed)}))
        return parts

    return with_connection(lambda: _snowflake_connect(cfg), "snowflake-connector-python", run)


TYPES: dict[str, Any] = {
    "redis": redis, "elasticsearch": elasticsearch, "clickhouse": clickhouse,
    "mysql": mysql, "mongodb": mongodb, "snowflake": snowflake,
}
# these open their own connection, so the generic TCP check would only duplicate it
OWN_CONNECTION = {"redis", "mysql", "mongodb"}
DEFAULT_INTERVALS = {"snowflake": 300}
