"""OTLP/HTTP receiver (JSON encoding) on :4318 for /v1/traces, /v1/logs and /v1/metrics.

Point instrumented services at it with:
    OTEL_EXPORTER_OTLP_ENDPOINT=http://<agent-host>:4318  OTEL_EXPORTER_OTLP_PROTOCOL=http/json

Sampling (blueprint section 6): every error span and every slow span is kept; healthy spans are
sampled per trace (deterministically by trace id, so a kept trace is kept whole).
Protobuf and gRPC (:4317) need the OpenTelemetry Collector in front of the agent; see README.
"""
import gzip
import json
import logging
import threading
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .store import STATUS_ERROR, Store

log = logging.getLogger("mdjaada.otlp")

_KINDS = {"SPAN_KIND_INTERNAL": 1, "SPAN_KIND_SERVER": 2, "SPAN_KIND_CLIENT": 3,
          "SPAN_KIND_PRODUCER": 4, "SPAN_KIND_CONSUMER": 5}
_STATUS = {"STATUS_CODE_UNSET": 0, "STATUS_CODE_OK": 1, "STATUS_CODE_ERROR": 2}
_SEVERITY = {"TRACE": 1, "DEBUG": 5, "INFO": 9, "WARN": 13, "WARNING": 13, "ERROR": 17, "FATAL": 21}
_SEVERITY_ENUM = {f"SEVERITY_NUMBER_{k}": v for k, v in _SEVERITY.items()}
PEER_ATTRS = ("peer.service", "messaging.destination.name", "messaging.destination", "server.address",
              "net.peer.name", "db.system")


def _value(v: dict):
    if not isinstance(v, dict):
        return v
    for key in ("stringValue", "boolValue", "doubleValue"):
        if key in v:
            return v[key]
    if "intValue" in v:
        return int(v["intValue"])
    if "arrayValue" in v:
        return [_value(x) for x in v["arrayValue"].get("values", [])]
    if "kvlistValue" in v:
        return attrs(v["kvlistValue"].get("values", []))
    return None


def attrs(items) -> dict:
    return {a.get("key"): _value(a.get("value", {})) for a in items or []}


def _enum(v, table: dict, default: int = 0) -> int:
    if isinstance(v, int):
        return v or default
    if isinstance(v, str):
        return int(v) if v.isdigit() else table.get(v, default)
    return default


def keep_span(trace_id: str, is_error: bool, duration_ms: float, slow_ms: float, sample_rate: float) -> bool:
    if is_error or duration_ms >= slow_ms or sample_rate >= 1:
        return True
    try:
        bucket = int(trace_id[-8:], 16) / 0xFFFFFFFF
    except ValueError:
        bucket = (hash(trace_id) & 0xFFFFFFFF) / 0xFFFFFFFF
    return bucket < sample_rate


def parse_traces(doc: dict, slow_ms: float = 500, sample_rate: float = 1.0) -> tuple[list[dict], int]:
    out, dropped = [], 0
    for rs in doc.get("resourceSpans", []):
        res = attrs(rs.get("resource", {}).get("attributes"))
        service = str(res.get("service.name", "unknown"))
        for ss in rs.get("scopeSpans", []) or rs.get("instrumentationLibrarySpans", []):
            for sp in ss.get("spans", []):
                a = attrs(sp.get("attributes"))
                start, end = int(sp.get("startTimeUnixNano", 0)), int(sp.get("endTimeUnixNano", 0))
                status = sp.get("status") or {}
                code = _enum(status.get("code"), _STATUS)
                duration = max(end - start, 0) / 1e6
                trace_id = str(sp.get("traceId", "")).lower()
                if not keep_span(trace_id, code == STATUS_ERROR, duration, slow_ms, sample_rate):
                    dropped += 1
                    continue
                peer = next((str(a[k]) for k in PEER_ATTRS if a.get(k)), "")
                out.append({
                    "trace_id": trace_id, "span_id": str(sp.get("spanId", "")).lower(),
                    "parent_span_id": str(sp.get("parentSpanId", "")).lower(), "service": service,
                    "name": sp.get("name", ""), "kind": _enum(sp.get("kind"), _KINDS), "start_ns": start,
                    "end_ns": end, "duration_ms": duration, "status_code": code,
                    "status_message": status.get("message", "") or str(a.get("exception.message", "")),
                    "peer": peer, "attrs": a,
                })
    return out, dropped


def parse_logs(doc: dict) -> list[dict]:
    out = []
    for rl in doc.get("resourceLogs", []):
        res = attrs(rl.get("resource", {}).get("attributes"))
        service = str(res.get("service.name", "unknown"))
        for sl in rl.get("scopeLogs", []):
            for r in sl.get("logRecords", []):
                text = (r.get("severityText") or "").upper()
                body = _value(r.get("body", {}))
                out.append({
                    "time_ns": int(r.get("timeUnixNano") or r.get("observedTimeUnixNano") or 0),
                    "service": service, "severity_text": text,
                    "severity_number": _enum(r.get("severityNumber"), _SEVERITY_ENUM, _SEVERITY.get(text, 0)),
                    "body": body if isinstance(body, str) else json.dumps(body),
                    "trace_id": str(r.get("traceId", "")).lower(), "span_id": str(r.get("spanId", "")).lower(),
                    "attrs": attrs(r.get("attributes")),
                })
    return out


_CUMULATIVE = {"AGGREGATION_TEMPORALITY_CUMULATIVE": 2, "AGGREGATION_TEMPORALITY_DELTA": 1}


def _num(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _scalar_attrs(a: dict) -> dict:
    return {k: v for k, v in a.items() if isinstance(v, (str, int, float, bool))}


def parse_metrics(doc: dict) -> list[dict]:
    """Gauges, sums (counters) and histograms. Each point keeps its attributes merged with the resource's
    (so rules can filter on service.name); summaries and exponential histograms keep only sum and count.
    A non-monotonic cumulative sum (UpDownCounter: pool usage, queue size) is a current level, so a gauge."""
    out = []
    for rm in doc.get("resourceMetrics", []):
        res = _scalar_attrs(attrs(rm.get("resource", {}).get("attributes")))
        service = str(res.get("service.name", "unknown"))
        for sm in rm.get("scopeMetrics", []) or rm.get("instrumentationLibraryMetrics", []):
            for m in sm.get("metrics", []):
                name = m.get("name", "")
                for field, kind in (("gauge", "gauge"), ("sum", "sum"), ("histogram", "histogram"),
                                    ("exponentialHistogram", "histogram"), ("summary", "histogram")):
                    data = m.get(field)
                    if data is None:
                        continue
                    # summaries are always cumulative; gauges have no temporality
                    cumulative = field == "summary" or _enum(data.get("aggregationTemporality"), _CUMULATIVE) == 2
                    if kind == "sum" and cumulative and not data.get("isMonotonic"):
                        kind = "gauge"
                    for dp in data.get("dataPoints", []):
                        p = {"time_ns": int(dp.get("timeUnixNano") or 0),
                             "start_ns": int(dp.get("startTimeUnixNano") or 0), "service": service, "name": name,
                             "kind": kind, "cumulative": int(cumulative and kind != "gauge"),
                             "attrs": {**res, **_scalar_attrs(attrs(dp.get("attributes")))},
                             "value": None, "count": None, "bounds": None, "buckets": None}
                        if kind == "histogram":
                            p["value"], p["count"] = _num(dp.get("sum")), _num(dp.get("count"))
                            if field == "histogram" and dp.get("explicitBounds") is not None:
                                p["bounds"] = [_num(b) for b in dp.get("explicitBounds", [])]
                                p["buckets"] = [_num(c) for c in dp.get("bucketCounts", [])]
                        else:
                            p["value"] = _num(dp["asDouble"] if "asDouble" in dp else dp.get("asInt"))
                        out.append(p)
                    break
    return out


def make_handler(store: Store, slow_ms: float, sample_rate: float):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # quiet
            pass

        def _reply(self, code: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            if self.path not in ("/v1/traces", "/v1/logs", "/v1/metrics"):
                return self._reply(404, {"error": "unknown path"})
            ctype = self.headers.get("Content-Type", "")
            if "json" not in ctype:
                return self._reply(415, {"error": "only OTLP/HTTP JSON is supported; set "
                                         "OTEL_EXPORTER_OTLP_PROTOCOL=http/json or put an OTel Collector in front"})
            raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            enc = self.headers.get("Content-Encoding", "")
            try:
                if enc == "gzip":
                    raw = gzip.decompress(raw)
                elif enc == "deflate":
                    raw = zlib.decompress(raw)
                doc = json.loads(raw or b"{}")
                if self.path == "/v1/traces":
                    rows, dropped = parse_traces(doc, slow_ms, sample_rate)
                    save = store.insert_spans
                elif self.path == "/v1/metrics":
                    rows, save = parse_metrics(doc), store.insert_metric_points
                else:
                    rows, save = parse_logs(doc), store.insert_logs
            except Exception as exc:
                log.warning("bad OTLP payload: %s", exc)
                return self._reply(400, {"error": str(exc)})
            try:
                save(rows)
            except Exception as exc:   # our own storage (e.g. a full disk), not the sender's payload: let it retry
                log.warning("could not store OTLP data: %s", exc)
                return self._reply(503, {"error": f"agent storage unavailable: {exc}"})
            return self._reply(200, {"partialSuccess": {}})

    return Handler


def serve(store: Store, port: int, host: str = "0.0.0.0", slow_ms: float = 500,
          sample_rate: float = 1.0) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), make_handler(store, slow_ms, sample_rate))
    threading.Thread(target=server.serve_forever, name="otlp-http", daemon=True).start()
    return server
