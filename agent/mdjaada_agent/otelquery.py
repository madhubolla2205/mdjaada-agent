"""Explore: charts for any OpenTelemetry metric the apps send, computed here, next to the raw points.

- catalog: every metric received recently, its kind, services and attribute keys (for "split by");
- series: one metric as lines over time: per step the same aggregation node rules use (gauge avg/max/min/sum,
  counter rate or increase, histogram p50/p90/p95/p99/avg/rate), optionally filtered (otel_filter syntax) and
  split by one attribute (a line per value, the busiest lines first).
Only the lines leave the agent, never the raw points.
"""
import bisect
import json
import math
from datetime import datetime, timezone

from .otelrules import AGGS, aggregate, default_agg, matches, parse_filter
from .store import Store, now_ns

MAX_MINUTES = 360        # the store keeps OTel metric points for 6 hours
MAX_STEPS = 120          # points per line
MAX_LINES = 12           # lines per chart; the rest are dropped, busiest kept
MAX_RAW = 60000          # raw points read per chart
KIND_AGGS = {"gauge": ("avg", "max", "min", "sum", "last"),
             "sum": ("rate", "increase", "last"),
             "histogram": ("p50", "p90", "p95", "p99", "avg", "rate", "increase")}


def _iso(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, timezone.utc).isoformat().replace("+00:00", "Z")


def _by_series(points: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for p in points:
        out.setdefault(json.dumps(p["attrs"], sort_keys=True), []).append(p)
    return out


def catalog(store: Store, since_ns: int) -> list[dict]:
    """Metrics received since since_ns: name, kind, services, points and their attribute keys."""
    metrics: dict[str, dict] = {}
    for r in store.metric_names(since_ns):
        m = metrics.setdefault(r["name"], {"name": r["name"], "kind": r["kind"], "services": [], "points": 0})
        if r["service"] not in m["services"]:
            m["services"].append(r["service"])
        m["points"] += r["points"]
    for name, m in metrics.items():
        values: dict[str, set] = {}
        for attrs in store.metric_attr_samples(name, since_ns, 300):
            for k, v in attrs.items():
                values.setdefault(k, set()).add(str(v))
        m["attributes"] = sorted(({"key": k, "values": len(v)} for k, v in values.items()),
                                 key=lambda a: (not 2 <= a["values"] <= 30, a["key"]))   # good "split by" keys first
        m["default_agg"] = "p95" if m["kind"] == "histogram" else default_agg(m["kind"], [])
        m["aggs"] = list(KIND_AGGS.get(m["kind"], ("avg",)))
    return sorted(metrics.values(), key=lambda m: m["name"])


def series(store: Store, payload: dict) -> dict:
    name = str(payload.get("metric") or "").strip()
    if not name:
        raise ValueError("metric is required")
    minutes = max(1.0, min(float(payload.get("minutes") or 60), MAX_MINUTES))
    end = now_ns()
    start = end - int(minutes * 60e9)
    step_s = max(10, int(math.ceil(minutes * 60 / MAX_STEPS / 10.0)) * 10)
    step = int(step_s * 1e9)
    flt = parse_filter(str(payload.get("filter") or ""))
    group_by = str(payload.get("group_by") or "").strip()
    # one step before the range too: the export just before a step is the baseline for counters and histograms
    pts = [p for p in store.metric_points(name, start - step, end, limit=MAX_RAW) if matches(p["attrs"], flt)]
    out = {"what": "metric_series", "metric": name, "window": {"start": _iso(start), "end": _iso(end)},
           "step_s": step_s, "group_by": group_by, "series": [], "truncated": False}
    if not pts:
        return out
    kind = pts[0]["kind"]
    agg = payload.get("agg") if payload.get("agg") in KIND_AGGS.get(kind, AGGS) else None
    agg = agg or ("p95" if kind == "histogram" and any(p["buckets"] for p in pts) else default_agg(kind, pts))
    out.update(kind=kind, agg=agg)

    groups: dict[str, list[dict]] = {}
    for p in pts:
        key = (str(p["attrs"].get(group_by)) if p["attrs"].get(group_by) is not None else "(none)") if group_by else "all"
        groups.setdefault(key, []).append(p)
    lines = []
    for key, g in groups.items():
        # rates add up per series: one series that just restarted must not shorten the others' time span
        parts = list(_by_series(g).values()) if agg == "rate" else [g]
        times = [[p["time_ns"] for p in part] for part in parts]
        line = []
        t = start + step
        while t <= end + step // 2:
            vals = []
            for part, ts in zip(parts, times):
                lo, hi = bisect.bisect_left(ts, t - 2 * step), bisect.bisect_right(ts, t)
                v = aggregate(part[lo:hi], agg, step_s, t - step) if hi > lo else None
                if v is not None:
                    vals.append(v)
            v = sum(vals) if vals else None
            line.append([_iso(min(t, end)), None if v is None else round(v, 6)])
            t += step
        values = [v for _, v in line if v is not None]
        if values:
            lines.append({"label": key, "points": line, "mean": sum(values) / len(values)})
    lines.sort(key=lambda s: -abs(s["mean"]))
    out["truncated"] = len(lines) > MAX_LINES
    out["series"] = [{"label": s["label"], "points": s["points"]} for s in lines[:MAX_LINES]]
    return out
