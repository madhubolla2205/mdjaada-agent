import json
import os
import socketserver
import sys
import threading
import time
import types
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mdjaada_agent import checks, stores  # noqa: E402


def run(node_type, **cfg):
    return checks.run_check({"node_id": f"t-{node_type}-{time.time_ns()}", "type": node_type, "config": cfg})


# ---------------------------------------------------------------- Redis (fake RESP server)
INFO_OK = "# Server\r\nredis_version:7.4.0\r\nrole:master\r\nconnected_clients:12\r\nblocked_clients:0\r\n" \
          "used_memory:500\r\nmaxmemory:1000\r\nevicted_keys:0\r\nrdb_last_bgsave_status:ok\r\n"


@pytest.fixture
def fake_redis():
    state = {"info": INFO_OK, "password": None}

    class H(socketserver.StreamRequestHandler):
        def handle(self):
            authed = state["password"] is None
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                n = int(line[1:])
                args = []
                for _ in range(n):
                    size = int(self.rfile.readline()[1:])
                    args.append(self.rfile.read(size + 2)[:-2].decode())
                cmd = args[0].upper()
                if cmd == "AUTH":
                    authed = args[-1] == state["password"]
                    self.wfile.write(b"+OK\r\n" if authed else b"-WRONGPASS invalid username-password pair\r\n")
                elif not authed:
                    self.wfile.write(b"-NOAUTH Authentication required.\r\n")
                elif cmd == "PING":
                    self.wfile.write(b"+PONG\r\n")
                elif cmd == "INFO":
                    data = state["info"].encode()
                    self.wfile.write(b"$%d\r\n%s\r\n" % (len(data), data))

    srv = socketserver.ThreadingTCPServer(("127.0.0.1", 0), H)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv.server_address[1], state
    srv.shutdown()


def test_redis_healthy_and_memory_pressure(fake_redis):
    port, state = fake_redis
    r = run("redis", host="127.0.0.1", port=port)
    assert r.ok and not r.warn and r.metrics["memory_used_pct"] == 50 and r.metrics["connected_clients"] == 12
    state["info"] = INFO_OK.replace("used_memory:500", "used_memory:950")
    r = run("redis", host="127.0.0.1", port=port)
    assert r.ok and r.warn and "memory used 95% > 90%" in r.message


def test_redis_auth_replica_and_unreachable(fake_redis):
    port, state = fake_redis
    state["password"] = "s3cret"
    assert not run("redis", host="127.0.0.1", port=port).ok  # NOAUTH
    r = run("redis", host="127.0.0.1", port=port, password="nope")
    assert not r.ok and "WRONGPASS" in r.message
    os.environ["FT_TEST_REDIS_PW"] = "s3cret"
    state["info"] = INFO_OK.replace("role:master", "role:slave\r\nmaster_link_status:down")
    r = run("redis", host="127.0.0.1", port=port, password="env:FT_TEST_REDIS_PW")
    assert not r.ok and "replica link to primary is down" in r.message
    r = run("redis", host="127.0.0.1", port=1)
    assert not r.ok and "connect failed" in r.message and "tcp" not in r.message  # no duplicate generic TCP check


def test_redis_evictions_between_checks(fake_redis):
    port, state = fake_redis
    node = {"node_id": "evict", "type": "redis", "config": {"host": "127.0.0.1", "port": port}}
    checks.run_check(node)
    state["info"] = INFO_OK.replace("evicted_keys:0", "evicted_keys:40")
    r = checks.run_check(node)
    assert r.warn and "40 keys evicted" in r.message


# ---------------------------------------------------------------- HTTP fakes (Elasticsearch, ClickHouse)
@pytest.fixture
def http():
    """handler(method, path, body) -> (status, body)"""
    box = {}

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _do(self):
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0)).decode()
            box["headers"] = {k.lower(): v for k, v in self.headers.items()}
            status, out = box["handler"](self.command, self.path, body)
            data = out.encode() if isinstance(out, str) else json.dumps(out).encode()
            self.send_response(status)
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = _do

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}", box
    srv.shutdown()


def es_handler(status="green", heap=40, latest_ms=None, nodes=3):
    def h(method, path, body):
        if path.startswith("/_cluster/health"):
            return 200, {"status": status, "number_of_nodes": nodes, "unassigned_shards": 0 if status == "green" else 4}
        if path.startswith("/_nodes/stats/jvm"):
            return 200, {"nodes": {"a": {"jvm": {"mem": {"heap_used_percent": heap}}}}}
        if "/_search" in path:
            assert json.loads(body)["aggs"]["latest"]["max"]["field"] == "@timestamp"
            return 200, {"aggregations": {"latest": {"value": latest_ms}}}
        return 404, {}
    return h


def test_elasticsearch_status_heap_and_freshness(http):
    url, box = http
    box["handler"] = es_handler()
    r = run("elasticsearch", url=url, api_key="abc")
    assert r.ok and not r.warn and r.metrics["cluster_status"] == "green"
    assert box["headers"]["authorization"] == "ApiKey abc"

    box["handler"] = es_handler(status="yellow", heap=92)
    r = run("elasticsearch", url=url)
    assert r.ok and r.warn and "cluster yellow" in r.message and "max JVM heap 92% > 85%" in r.message

    box["handler"] = es_handler(status="red")
    assert not run("elasticsearch", url=url).ok

    old = (time.time() - 600) * 1000
    box["handler"] = es_handler(latest_ms=old)
    r = run("elasticsearch", url=url, index="orders", freshness_s_fail=300)
    assert not r.ok and "orders newest doc age" in r.message

    box["handler"] = es_handler(nodes=2)
    assert not run("elasticsearch", url=url, expected_nodes=3).ok


def ch_handler(answers):
    def h(method, path, body):
        sql = body.replace(" FORMAT TabSeparated", "")
        for needle, value in answers.items():
            if needle in sql:
                if isinstance(value, Exception):
                    return 500, f"Code: 60. DB::Exception: {value}"
                return 200, f"{value}\n"
        return 200, "0\n"
    return h


def test_clickhouse_checks(http):
    url, box = http
    fresh = (datetime.now(timezone.utc) - timedelta(seconds=20)).strftime("%Y-%m-%d %H:%M:%S")
    box["handler"] = ch_handler({"SELECT 1": 1, "max(ts)": fresh, "system.replicas": 0, "system.parts": 12})
    r = run("clickhouse", url=url, username="reader", password="pw", freshness_query="SELECT max(ts) FROM events",
            freshness_s_fail=120)
    assert r.ok and not r.warn and 15 <= r.metrics["data_age_s"] <= 60 and r.metrics["max_parts_per_partition"] == 12
    assert box["headers"]["x-clickhouse-user"] == "reader"

    box["handler"] = ch_handler({"SELECT 1": 1, "SELECT count() FROM system.replicas": 4, "countIf(is_readonly)": 1,
                                 "absolute_delay": 3, "system.parts": 450})
    r = run("clickhouse", url=url)
    assert not r.ok and "read-only" in r.message
    assert r.metrics["max_parts_per_partition"] == 450

    box["handler"] = ch_handler({"SELECT 1": 1, "events": RuntimeError("Table default.events does not exist")})
    r = run("clickhouse", url=url, freshness_query="SELECT max(ts) FROM events")
    assert not r.ok and "Table default.events does not exist" in r.message


# ---------------------------------------------------------------- driver-based stores (fake drivers)
class FakeCursor:
    def __init__(self, answer):
        self.answer, self.description, self._rows = answer, None, []

    def execute(self, sql):
        cols, rows = self.answer(sql)
        self.description = [(c,) for c in cols]
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class FakeConn:
    def __init__(self, answer):
        self.answer, self.closed = answer, False

    def cursor(self):
        return FakeCursor(self.answer)

    def close(self):
        self.closed = True


def test_missing_driver_is_a_warning_not_an_outage(monkeypatch):
    monkeypatch.setitem(sys.modules, "pymysql", None)  # import fails
    r = run("mysql", host="db")
    assert r.ok and r.warn and "install PyMySQL" in r.message


def mysql_answer(replica_running="Yes", lag=0, threads=20):
    def answer(sql):
        if sql.startswith("SHOW GLOBAL STATUS"):
            return ["Variable_name", "Value"], [("Threads_connected", str(threads)), ("Threads_running", "3")]
        if "@@max_connections" in sql:
            return ["@@max_connections"], [(100,)]
        if sql.startswith("SHOW REPLICA STATUS"):
            return ["Replica_IO_Running", "Replica_SQL_Running", "Seconds_Behind_Source", "Last_Error"], \
                [(replica_running, "Yes", lag, "")]
        if "max(created_at)" in sql:
            return ["m"], [(datetime.now(timezone.utc) - timedelta(seconds=5),)]
        return ["1"], [(1,)]
    return answer


def test_mysql_connections_replication_and_freshness(monkeypatch):
    conns = []

    def connect(**kw):
        assert kw["password"] == "pw" and kw["port"] == 3306
        c = FakeConn(state["answer"])
        conns.append(c)
        return c

    state = {"answer": mysql_answer()}
    monkeypatch.setitem(sys.modules, "pymysql", types.SimpleNamespace(connect=connect))
    os.environ["FT_TEST_MYSQL_PW"] = "pw"
    base = {"host": "db", "password": "env:FT_TEST_MYSQL_PW", "replica": "true",
            "freshness_query": "select max(created_at) from orders", "freshness_s_fail": 60}
    r = run("mysql", **base)
    assert r.ok and not r.warn and r.metrics["connections_used_pct"] == 20 and r.metrics["data_age_s"] <= 10
    assert all(c.closed for c in conns)

    state["answer"] = mysql_answer(threads=95, lag=400)
    r = run("mysql", **base, replication_lag_s_warn=60)
    assert r.ok and r.warn and "connections used 95% > 85%" in r.message and "replication lag 400 s" in r.message

    state["answer"] = mysql_answer(replica_running="No")
    r = run("mysql", **base)
    assert not r.ok and "replication stopped (IO=No" in r.message


class FakeMongo:
    def __init__(self, rs=None, newest=None, conns=(10, 90)):
        self.rs, self.newest, self.conns = rs, newest, conns
        self.admin = self

    def command(self, name):
        if name == "ping":
            return {"ok": 1}
        if name == "serverStatus":
            return {"connections": {"current": self.conns[0], "available": self.conns[1]}}
        if name == "replSetGetStatus":
            if self.rs is None:
                e = RuntimeError("not running with --replSet")
                e.code = 76
                raise e
            return self.rs

    def __getitem__(self, _):
        return self

    def find_one(self, *a, **kw):
        return {"ts": self.newest} if self.newest else None

    def close(self):
        pass


def test_mongodb_standalone_replset_and_freshness(monkeypatch):
    now = datetime.now(timezone.utc).replace(tzinfo=None)  # pymongo returns naive UTC datetimes
    fake = {"c": FakeMongo(newest=now - timedelta(seconds=30))}
    monkeypatch.setattr(stores, "_mongo_connect", lambda cfg: fake["c"])
    r = run("mongodb", uri="mongodb://x", database="shop", collection="events", freshness_field="ts", freshness_s_warn=10)
    assert r.ok and r.warn and "events newest doc age" in r.message and r.metrics["connections_used_pct"] == 10

    members = [{"name": "a:27017", "stateStr": "PRIMARY", "health": 1, "optimeDate": now},
               {"name": "b:27017", "stateStr": "SECONDARY", "health": 1, "optimeDate": now - timedelta(seconds=45)},
               {"name": "c:27017", "stateStr": "(not reachable/healthy)", "health": 0}]
    fake["c"] = FakeMongo(rs={"set": "rs0", "members": members})
    r = run("mongodb", uri="mongodb://x", replication_lag_s_fail=30)
    assert not r.ok and "max secondary lag 45 s > 30 s" in r.message and "unhealthy members: c:27017" in r.message

    fake["c"] = FakeMongo(rs={"set": "rs0", "members": [{"name": "b", "stateStr": "SECONDARY", "health": 1}]})
    assert "has no PRIMARY" in run("mongodb", uri="mongodb://x").message


def test_snowflake_tasks_and_default_interval(monkeypatch):
    def answer(sql):
        if "TASK_HISTORY" in sql:
            return ["c"], [(2,)]
        return ["1"], [(1,)]
    monkeypatch.setattr(stores, "_snowflake_connect", lambda cfg: FakeConn(answer))
    r = run("snowflake", account="xy12345", user="monitor", check_tasks="true")
    assert not r.ok and "2 failed tasks in the last hour" in r.message
    assert stores.DEFAULT_INTERVALS["snowflake"] == 300
    assert run("snowflake") is None  # not configured


def test_empty_ui_values_fall_back_to_default_thresholds(fake_redis):
    port, state = fake_redis
    state["info"] = INFO_OK.replace("used_memory:500", "used_memory:950")
    r = run("redis", host="127.0.0.1", port=port, memory_pct_warn="")
    assert r.warn
