"""OTel metric rules: any node can be judged on a metric its applications send over OTLP.

Node settings (all optional except otel_metric):
  otel_metric        metric name, e.g. http.server.request.duration
  otel_filter        attribute filter, e.g. service.name=orders-api, http.route=/pay, http.response.status_code=~5..
                     (= equals, != not equal, =~ regex; resource attributes such as service.name are included)
  otel_agg           avg | max | min | last | sum | rate | increase | p50 | p90 | p95 | p99
                     (defaults: gauge avg, counter rate, histogram p95 or avg when it has no buckets)
  otel_ratio_of      a second filter: the value becomes 100 * (points matching otel_filter) / (points matching this),
                     e.g. error rate in %: otel_filter "...,http.response.status_code=~5.." otel_ratio_of "service.name=orders-api"
  otel_window_s      how far back to look (default 180; counters and histograms need two exports in the window)
  otel_scale         multiply the value, e.g. 1000 to turn seconds into ms
  otel_label         name shown on the node and its chart
  otel_warn / otel_fail / otel_warn_below / otel_fail_below   limits (see checks.thresholds)
  otel_missing       warn (default) | ok | fail  when no points arrived in the window
"""
import json
import re
import time
from typing import Optional

from .checks import Result, get, num, thresholds

store = None  # set by the agent: the local telemetry store the OTLP receiver writes to
AGGS = ("avg", "max", "min", "last", "sum", "rate", "increase", "p50", "p90", "p95", "p99")
_FILTER = re.compile(r"\s*([^=!~,\s]+)\s*(=~|!=|=)\s*([^,]*)")


def parse_filter(text: str) -> list[tuple[str, str, str]]:
    out = []
    for part in (text or "").split(","):
        if not part.strip():
            continue
        m = _FILTER.fullmatch(part)
        if not m:
            raise ValueError(f"bad filter {part.strip()!r}: use key=value, key!=value or key=~regex")
        out.append((m.group(1), m.group(2), m.group(3).strip().strip('"\'')))
    return out


def matches(attrs: dict, flt: list[tuple[str, str, str]]) -> bool:
    for key, op, want in flt:
        have = attrs.get(key)
        have = "" if have is None else str(have).lower() if isinstance(have, bool) else str(have)
        if op == "=" and have != want:
            return False
        if op == "!=" and have == want:
            return False
        if op == "=~" and not re.fullmatch(want, have):
            return False
    return True


def _series(points: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for p in points:
        out.setdefault(json.dumps(p["attrs"], sort_keys=True), []).append(p)
    return out


def _diff(first: list, last: list) -> list:
    if not first or len(first) != len(last):
        return list(last)
    d = [b - a for a, b in zip(first, last)]
    return list(last) if any(x < 0 for x in d) else d  # counter reset: the new values are the increase


def quantile(q: float, bounds: list[float], counts: list[float]) -> Optional[float]:
    """Linear interpolation inside the bucket that holds the q-th observation (as Prometheus does)."""
    total = sum(counts)
    if total <= 0:
        return None
    rank, seen = q * total, 0.0
    for i, c in enumerate(counts):
        if seen + c >= rank and c > 0:
            lo = bounds[i - 1] if i > 0 else 0.0
            if i >= len(bounds):  # overflow bucket: the best estimate is its lower edge
                return lo
            return lo + (bounds[i] - lo) * (rank - seen) / c
        seen += c
    return bounds[-1] if bounds else None


def aggregate(points: list[dict], agg: str, window_s: float, window_start_ns: int = 0) -> Optional[float]:
    """One number from all matching points of one metric in the window."""
    before = [p for p in points if p["time_ns"] <= window_start_ns]  # only baselines for cumulative series
    points = [p for p in points if p["time_ns"] > window_start_ns]
    if not points:
        return None
    kind = points[0]["kind"]
    groups = _series(points)
    baselines = {k: g[-1] for k, g in _series([p for p in before if p["cumulative"]]).items()}
    if kind == "gauge":
        if agg in ("last", "sum", "increase"):
            return sum(g[-1]["value"] for g in groups.values())
        vals = [p["value"] for p in points]
        return {"max": max, "min": min}.get(agg, lambda v: sum(v) / len(v))(vals)

    # counters and histograms: the increase over the window, per series, then summed
    increase = span_s = 0.0
    total_sum = total_count = 0.0
    bucket_bounds, bucket_counts = None, None
    for key, g in groups.items():
        if g[0]["cumulative"]:
            last = g[-1]
            if key in baselines and baselines[key].get("start_ns") == g[0].get("start_ns"):
                first = baselines[key]  # the last export before the window: the increase covers all of it
            elif (g[0].get("start_ns") or 0) >= window_start_ns > 0:
                # the series began inside the window (e.g. the first 503 since the app started): all of it counts
                first = {"time_ns": g[0]["start_ns"], "value": 0.0, "count": 0.0,
                         "buckets": [0.0] * len(last["buckets"]) if last["buckets"] else None}
            elif len(g) < 2:
                continue  # one export of an older cumulative series says nothing about the window yet
            else:
                first = g[0]
            span_s = max(span_s, (last["time_ns"] - first["time_ns"]) / 1e9)
            dv = last["value"] - first["value"]
            dv = last["value"] if dv < 0 else dv
            dc = (last["count"] or 0) - (first["count"] or 0)
            dc = (last["count"] or 0) if dc < 0 else dc
            db = _diff(first["buckets"], last["buckets"]) if last["buckets"] else None
        else:
            span_s = max(span_s, window_s)
            dv = sum(p["value"] for p in g)
            dc = sum(p["count"] or 0 for p in g)
            db = [sum(c) for c in zip(*[p["buckets"] for p in g])] if all(p["buckets"] for p in g) else None
        if kind == "sum":
            increase += dv
            continue
        total_sum, total_count = total_sum + dv, total_count + dc
        if db is not None:
            bounds = g[-1]["bounds"]
            if bucket_counts is None:
                bucket_bounds, bucket_counts = bounds, db
            elif bounds == bucket_bounds and len(db) == len(bucket_counts):
                bucket_counts = [a + b for a, b in zip(bucket_counts, db)]

    if kind == "sum":
        if agg == "last":
            return sum(g[-1]["value"] for g in groups.values())
        if agg in ("increase", "sum"):
            return increase
        return increase / span_s if span_s > 0 else None  # rate per second
    # histogram
    if agg == "rate":
        return total_count / span_s if span_s > 0 else None
    if agg in ("increase", "sum"):
        return total_count
    if agg.startswith("p") and bucket_counts:
        return quantile(int(agg[1:]) / 100, bucket_bounds, bucket_counts)
    return total_sum / total_count if total_count > 0 else (0.0 if span_s > 0 else None)


def default_agg(kind: str, points: list[dict]) -> str:
    if kind == "sum":
        return "rate"
    if kind == "histogram":
        return "p95" if any(p["buckets"] for p in points) else "avg"
    return "avg"


def value_for(name: str, flt: list, agg: Optional[str], window_s: float, now_ns: Optional[int] = None):
    now_ns = now_ns or time.time_ns()
    start_ns = now_ns - int(window_s * 1e9)
    # one more window back: the export just before the window is the baseline for counters and histograms
    pts = [p for p in store.metric_points(name, start_ns - int(window_s * 1e9), now_ns) if matches(p["attrs"], flt)]
    if not any(p["time_ns"] > start_ns for p in pts):
        return None, agg
    agg = agg or default_agg(pts[0]["kind"], pts)
    return aggregate(pts, agg, window_s, start_ns), agg


def check(cfg: dict) -> Optional[Result]:
    name = get(cfg, "otel_metric")
    if not name:
        return None
    if store is None:
        return Result(True, "OTel metrics need the agent's OTLP receiver (MDJAADA_OTLP_PORT)", warn=True)
    try:
        flt = parse_filter(get(cfg, "otel_filter", ""))
        ratio_of = parse_filter(get(cfg, "otel_ratio_of", "")) if get(cfg, "otel_ratio_of") else None
    except ValueError as exc:
        return Result(False, str(exc))
    agg = get(cfg, "otel_agg") or None
    if agg and agg not in AGGS:
        return Result(False, f"unknown aggregation {agg!r}; use one of {', '.join(AGGS)}")
    window = num(cfg, "otel_window_s") or 180
    if ratio_of is not None:
        # compare counts over the same window: rates of series that started at different times would not divide
        count_agg = "increase" if agg in (None, "rate", "sum") else agg
        value, _ = value_for(name, flt, count_agg, window)
        total, _ = value_for(name, ratio_of, count_agg, window)
        if total is None:
            value = None
        else:  # no matching points (e.g. no errors at all) or no traffic = 0 %
            value = 100 * (value or 0) / total if total else 0.0
    else:
        value, agg = value_for(name, flt, agg, window)
    label = get(cfg, "otel_label") or (f"{name} {agg or ''}".strip() + (" %" if ratio_of is not None else ""))
    if value is None:
        missing = str(get(cfg, "otel_missing", "warn")).lower()
        msg = f"no OTel data for {name} in the last {window:g} s"
        return Result(missing != "fail", msg, warn=missing == "warn")
    value *= num(cfg, "otel_scale") or 1
    r = thresholds(value, cfg, "otel", label)
    r.metrics = {label: round(value, 4)}
    return r
