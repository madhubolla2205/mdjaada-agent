"""Node library, part 4: platforms.

| type          | what it watches                                                   | access                                  |
|---------------|-------------------------------------------------------------------|-----------------------------------------|
| kubernetes    | a Deployment / StatefulSet / DaemonSet (or pods by label)         | Kubernetes API, standard library only   |
| cloudwatch    | CloudWatch alarm state, or any CloudWatch metric with thresholds  | boto3 (AWS credential chain)            |
| azure_monitor | any Azure Monitor resource metric, or a Log Analytics KQL query   | azure-monitor-querymetrics (metrics),   |
|               |                                                                   | azure-monitor-query (logs), azure-identity |

Kubernetes credentials, in order: `api_url` + `token`, a kubeconfig (`kubeconfig` setting or $KUBECONFIG, needs PyYAML),
or the in-cluster ServiceAccount when the agent runs as a pod. Kubernetes rollouts are reported as change events so
incidents can correlate them like CI deploys. `discover_kubernetes()` turns Services into suggested nodes.
"""
import base64
import json
import os
import ssl
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from . import USER_AGENT
from .checks import TIMEOUT, Result, _memory, err, get, num, thresholds, truthy

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
BAD_WAITING = {"CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError",
               "CreateContainerError", "InvalidImageName", "RunContainerError"}
SYSTEM_NAMESPACES = {"kube-system", "kube-public", "kube-node-lease", "local-path-storage"}


# ---------------------------------------------------------------- Kubernetes API client
class Kube:
    def __init__(self, server: str, token: Optional[str] = None, ca_file: Optional[str] = None,
                 insecure: bool = False, cert: Optional[tuple[str, str]] = None):
        self.server, self.token = server.rstrip("/"), token
        self.ctx = ssl.create_default_context(cafile=ca_file) if ca_file else ssl.create_default_context()
        if insecure:
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE
        if cert:
            self.ctx.load_cert_chain(*cert)

    def get(self, path: str, **params) -> dict:
        url = self.server + path + ("?" + urllib.parse.urlencode(params) if params else "")
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT,
                                        context=self.ctx if url.startswith("https") else None) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            try:
                message = json.loads(exc.read()).get("message", "")
            except Exception:
                message = ""
            raise RuntimeError(f"HTTP {exc.code} {message}".strip()) from None


def _tmp(data_b64: str, suffix: str) -> str:
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "wb") as f:
        f.write(base64.b64decode(data_b64))
    os.chmod(path, 0o600)
    return path


def _from_kubeconfig(path: str, context: Optional[str]) -> Kube:
    try:
        import yaml  # type: ignore
    except ImportError:
        raise RuntimeError("reading a kubeconfig needs PyYAML on the agent host (pip install pyyaml)") from None
    with open(os.path.expanduser(path)) as f:
        doc = yaml.safe_load(f)

    def pick(section: str, name: str) -> dict:
        for item in doc.get(section) or []:
            if item.get("name") == name:
                return item.get(section[:-1]) or {}
        raise RuntimeError(f"kubeconfig has no {section[:-1]} named {name!r}")

    ctx = pick("contexts", context or doc.get("current-context", ""))
    cluster, user = pick("clusters", ctx["cluster"]), pick("users", ctx["user"])
    if "exec" in user and not user.get("token"):
        raise RuntimeError("this kubeconfig user signs in with an exec plugin (EKS/GKE/AKS); give the agent a "
                           "ServiceAccount token instead")
    ca = cluster.get("certificate-authority") or (
        _tmp(cluster["certificate-authority-data"], ".crt") if cluster.get("certificate-authority-data") else None)
    cert = None
    if user.get("client-certificate-data") or user.get("client-certificate"):
        cert = (user.get("client-certificate") or _tmp(user["client-certificate-data"], ".crt"),
                user.get("client-key") or _tmp(user["client-key-data"], ".key"))
    token = user.get("token")
    if not token and user.get("tokenFile"):
        token = open(user["tokenFile"]).read().strip()
    return Kube(cluster["server"], token, ca, bool(cluster.get("insecure-skip-tls-verify")), cert)


_kube_cache: dict[str, Kube] = {}


def kube(cfg: dict) -> Kube:
    key = json.dumps({k: get(cfg, k) for k in ("api_url", "token", "ca_file", "insecure", "kubeconfig", "context")})
    if key in _kube_cache:
        return _kube_cache[key]
    if get(cfg, "api_url"):
        k = Kube(get(cfg, "api_url"), get(cfg, "token"), get(cfg, "ca_file"), truthy(get(cfg, "insecure")))
    elif get(cfg, "kubeconfig") or os.environ.get("KUBECONFIG"):
        k = _from_kubeconfig(get(cfg, "kubeconfig") or os.environ["KUBECONFIG"].split(os.pathsep)[0], get(cfg, "context"))
    elif os.environ.get("KUBERNETES_SERVICE_HOST") and os.path.exists(f"{SA_DIR}/token"):
        host, port = os.environ["KUBERNETES_SERVICE_HOST"], os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        k = Kube(f"https://{host}:{port}", open(f"{SA_DIR}/token").read().strip(), f"{SA_DIR}/ca.crt")
    else:
        raise RuntimeError("no Kubernetes credentials: run the agent in the cluster, set KUBECONFIG, "
                           "or set api_url + token on the node")
    _kube_cache[key] = k
    return k


def _ts(value: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


# ---------------------------------------------------------------- Kubernetes workload check
PLURAL = {"deployment": "deployments", "statefulset": "statefulsets", "daemonset": "daemonsets"}


def kubernetes(node_id: str, cfg: dict) -> list[Result]:
    name, selector = get(cfg, "name"), get(cfg, "selector")
    if not (name or selector):
        return []
    ns = get(cfg, "namespace", "default")
    kind = str(get(cfg, "kind", "deployment")).lower()
    try:
        k = kube(cfg)
        start = time.monotonic()
        workload = k.get(f"/apis/apps/v1/namespaces/{ns}/{PLURAL.get(kind, kind + 's')}/{name}") if name else None
        ms = (time.monotonic() - start) * 1000
    except Exception as exc:
        what = f"{kind}/{name}" if name else f"pods {selector}"
        return [Result(False, f"{ns}/{what}: {err(exc)}")]
    parts: list[Result] = []

    if workload:
        spec, status = workload.get("spec", {}), workload.get("status", {})
        if kind == "daemonset":
            desired, ready = status.get("desiredNumberScheduled", 0), status.get("numberReady", 0)
            updated = status.get("updatedNumberScheduled", 0)
        else:
            desired, ready = spec.get("replicas", 1), status.get("readyReplicas", 0)
            updated = status.get("updatedReplicas", 0)
        metrics = {"ready": ready, "desired": desired}
        min_ready = num(cfg, "min_ready") or 0
        scaled_zero = str(get(cfg, "alert_scaled_to_zero", "")).strip().lower()
        if desired == 0 and (min_ready > 0 or scaled_zero == "fail"):
            # a database or proxy scaled to 0 is an outage, not a healthy idle workload
            r = Result(False, f"{kind}/{name} scaled to 0" + (f", below minimum {min_ready:g}" if min_ready else ""))
        elif desired == 0:
            r = Result(True, f"{kind}/{name} scaled to 0", warn=truthy(scaled_zero))
        elif ready == 0:
            r = Result(False, f"0/{desired} pods ready")
        elif ready < min_ready:
            r = Result(False, f"{ready}/{desired} ready, below minimum {min_ready:g}")
        else:
            r = Result(True, f"{ready}/{desired} ready", warn=ready < desired)
        r.latency_ms, r.metrics = ms, metrics
        parts.append(r)
        for c in status.get("conditions") or []:
            if c.get("reason") == "ProgressDeadlineExceeded":
                parts.append(Result(False, f"rollout stuck: {c.get('message', '')[:160]}"))
        if status.get("observedGeneration", 0) < workload["metadata"].get("generation", 0) or updated < desired:
            parts.append(Result(True, f"rollout in progress ({updated}/{desired} updated)"))

        # rollouts become change events, like a deploy posted by CI
        images = ", ".join(c.get("image", "") for c in spec.get("template", {}).get("spec", {}).get("containers", []))
        version = (workload["metadata"].get("generation"), images)
        prev = _memory.get(f"{node_id}:k8s-version")
        _memory[f"{node_id}:k8s-version"] = version
        if prev and prev != version:
            what = f"image {images}" if prev[1] != images else f"spec change (generation {version[0]})"
            parts[0].events.append({"kind": "deploy", "message": f"rollout of {kind}/{name}: {what}"})

        labels = (spec.get("selector") or {}).get("matchLabels") or {}
        selector = ",".join(f"{k}={v}" for k, v in labels.items())

    try:
        pods = k.get(f"/api/v1/namespaces/{ns}/pods", labelSelector=selector)["items"] if selector else []
    except Exception as exc:
        return parts + [Result(True, f"could not list pods: {err(exc)}", warn=True)]

    if not workload:  # selector-only node
        ready = sum(1 for p in pods if any(c.get("type") == "Ready" and c.get("status") == "True"
                                           for c in p.get("status", {}).get("conditions") or []))
        parts.append(Result(ready > 0, f"{ready}/{len(pods)} pods ready ({selector})", warn=0 < ready < len(pods),
                            latency_ms=ms, metrics={"ready": ready, "desired": len(pods)}))

    restarts, problems, oom = 0, [], []
    now = datetime.now(timezone.utc)
    pending_limit = num(cfg, "pending_s") or 120
    for p in pods:
        pname, pstatus = p["metadata"]["name"], p.get("status", {})
        for cs in pstatus.get("containerStatuses") or []:
            restarts += cs.get("restartCount", 0)
            reason = ((cs.get("state") or {}).get("waiting") or {}).get("reason")
            if reason in BAD_WAITING:
                problems.append(f"{pname}: {reason}")
            if ((cs.get("lastState") or {}).get("terminated") or {}).get("reason") == "OOMKilled":
                oom.append(pname)
        created = _ts(p["metadata"].get("creationTimestamp"))
        if pstatus.get("phase") == "Pending" and created and (now - created).total_seconds() > pending_limit:
            why = next((c.get("reason") or c.get("message", "") for c in pstatus.get("conditions") or []
                        if c.get("status") == "False"), "")
            problems.append(f"{pname}: Pending {int((now - created).total_seconds())}s {why}".strip())

    prev = _memory.get(f"{node_id}:restarts")
    _memory[f"{node_id}:restarts"] = restarts
    new = max(restarts - prev, 0) if prev is not None else 0
    r = thresholds(new, {**cfg, "restarts_warn": get(cfg, "restarts_warn", 0)}, "restarts", "restarts since last check")
    r.metrics = {"restarts_total": restarts, "restarts_since_last": new, "pods": len(pods)}
    parts.append(r)
    if problems:
        all_bad = len({x.split(":")[0] for x in problems}) >= len(pods) > 0
        parts.append(Result(not all_bad, "; ".join(problems[:4]), warn=True))
    if oom:
        parts.append(Result(True, f"OOMKilled recently: {', '.join(oom[:3])}", warn=True, metrics={"oom_killed": len(oom)}))

    if any(not x.ok or x.warn for x in parts):  # add the cluster's own explanation when something is wrong
        try:
            evs = k.get(f"/api/v1/namespaces/{ns}/events", fieldSelector="type=Warning")["items"]
            prefix = name or ""
            current = {p["metadata"]["name"] for p in pods}
            recent = []
            for e in sorted(evs, key=lambda e: e.get("lastTimestamp") or e.get("eventTime") or "", reverse=True):
                when = _ts(e.get("lastTimestamp") or e.get("eventTime"))
                obj = e.get("involvedObject") or {}
                # pod events only for pods that still exist: a crash of a pod an earlier rollout replaced is history
                ours = obj.get("name", "") in current if obj.get("kind") == "Pod" else obj.get("name", "").startswith(prefix)
                if when and (now - when) < timedelta(minutes=10) and ours:
                    text = f"{e.get('reason')}: {(e.get('message') or '')[:100]}"
                    if text not in recent:
                        recent.append(text)
            if recent:
                # only added when the node is already unhealthy, so warn=True just makes it visible
                parts.append(Result(True, "events: " + " | ".join(recent[:3]), warn=True,
                                    metrics={"warning_events": len(recent)}))
        except Exception:
            pass  # events are an extra; RBAC may not allow them
    return parts


def discover_kubernetes(cfg: dict, namespaces: list[str]) -> list[dict]:
    """Services (and the workload behind each) as suggested nodes."""
    k = kube(cfg)
    if namespaces == ["all"]:
        namespaces = [n["metadata"]["name"] for n in k.get("/api/v1/namespaces")["items"]
                      if n["metadata"]["name"] not in SYSTEM_NAMESPACES]
    found = []
    for ns in namespaces:
        workloads = []
        for kind in ("deployment", "statefulset", "daemonset"):
            for w in k.get(f"/apis/apps/v1/namespaces/{ns}/{PLURAL[kind]}")["items"]:
                labels = w["spec"].get("template", {}).get("metadata", {}).get("labels") or {}
                workloads.append((kind, w["metadata"]["name"], labels))
        for svc in k.get(f"/api/v1/namespaces/{ns}/services")["items"]:
            sname, sel = svc["metadata"]["name"], svc["spec"].get("selector") or {}
            if not sel or (ns == "default" and sname == "kubernetes"):
                continue
            match = next(((kd, wn) for kd, wn, labels in workloads if sel.items() <= labels.items()), None)
            ports = svc["spec"].get("ports") or [{}]
            config = {"namespace": ns, "service": sname, "host": f"{sname}.{ns}.svc"}
            if ports[0].get("port"):
                # informational: a "port" setting would add a TCP check, which is wrong for workers whose
                # Service has no listener; users can opt in by setting port themselves
                config["service_port"] = str(ports[0]["port"])
            if match:
                # a Service is there to be reached: zero ready pods (crashed or scaled to 0) means it is down
                config.update(kind=match[0], name=match[1], min_ready="1")
            else:
                config["selector"] = ",".join(f"{k}={v}" for k, v in sel.items())
            found.append({"key": f"k8s/{ns}/service/{sname}", "name": sname, "type": "kubernetes",
                          "config": config, "source": "kubernetes"})
    return found


# ---------------------------------------------------------------- AWS CloudWatch
def _aws(cfg: dict, service: str):
    from .cloud import aws_client
    return aws_client(cfg, service)


def _dimensions(raw: str) -> list[dict]:
    out = []
    for part in filter(None, (p.strip() for p in str(raw or "").split(","))):
        k, _, v = part.partition("=")
        out.append({"Name": k.strip(), "Value": v.strip()})
    return out


def cloudwatch(node_id: str, cfg: dict) -> list[Result]:
    names = [a.strip() for a in str(get(cfg, "alarm_names", "")).split(",") if a.strip()]
    prefix, namespace, metric = get(cfg, "alarm_prefix"), get(cfg, "namespace"), get(cfg, "metric_name")
    if not (names or prefix or (namespace and metric)):
        return []
    try:
        client = _aws(cfg, "cloudwatch")
    except ImportError:
        return [Result(True, "install boto3 on the agent host to enable CloudWatch checks", warn=True)]
    except Exception as exc:
        return [Result(False, f"AWS client setup failed: {err(exc)}")]
    parts: list[Result] = []

    if names or prefix:
        try:
            start = time.monotonic()
            resp = client.describe_alarms(**({"AlarmNames": names} if names else {"AlarmNamePrefix": prefix}),
                                          MaxRecords=100)
            ms = (time.monotonic() - start) * 1000
        except Exception as exc:
            return [Result(False, f"CloudWatch alarms: {err(exc)}")]
        alarms = resp.get("MetricAlarms", []) + resp.get("CompositeAlarms", [])
        firing = [a for a in alarms if a.get("StateValue") == "ALARM"]
        unknown = [a["AlarmName"] for a in alarms if a.get("StateValue") == "INSUFFICIENT_DATA"]
        missing = sorted(set(names) - {a["AlarmName"] for a in alarms})
        metrics = {"alarms": len(alarms), "alarms_firing": len(firing)}
        if firing:
            parts.append(Result(False, "ALARM: " + "; ".join(f"{a['AlarmName']} ({a.get('StateReason', '')[:100]})"
                                                             for a in firing[:3]), latency_ms=ms, metrics=metrics))
        else:
            parts.append(Result(True, f"{len(alarms)} alarms OK", latency_ms=ms, metrics=metrics))
        if unknown:
            parts.append(Result(True, f"insufficient data: {', '.join(unknown[:3])}", warn=True))
        if missing:
            parts.append(Result(True, f"alarms not found: {', '.join(missing[:3])}", warn=True))

    if namespace and metric:
        stat = get(cfg, "statistic", "Average")
        period = int(num(cfg, "period") or 60)
        now = datetime.now(timezone.utc)
        kw: dict[str, Any] = {"Namespace": namespace, "MetricName": metric, "Dimensions": _dimensions(get(cfg, "dimensions")),
                              "StartTime": now - timedelta(seconds=max(period * 10, 900)), "EndTime": now,
                              "Period": period}
        if str(stat).lower().startswith("p"):
            kw["ExtendedStatistics"] = [stat]
        else:
            kw["Statistics"] = [stat]
        try:
            start = time.monotonic()
            points = sorted(client.get_metric_statistics(**kw).get("Datapoints", []), key=lambda p: p["Timestamp"])
            ms = (time.monotonic() - start) * 1000
        except Exception as exc:
            return parts + [Result(False, f"CloudWatch {namespace}/{metric}: {err(exc)}")]
        label = get(cfg, "value_label") or f"{metric} {stat}"
        if not points:
            parts.append(Result(True, f"no {namespace}/{metric} datapoints in the last {max(period * 10, 900) // 60} min",
                                warn=True, latency_ms=ms))
        else:
            last = points[-1]
            value = float(last["ExtendedStatistics"][stat] if "ExtendedStatistics" in kw else last[stat])
            r = thresholds(value, cfg, "value", label)
            r.latency_ms, r.metrics = ms, {label: value}
            parts.append(r)
    return parts


# ---------------------------------------------------------------- Azure Monitor
_azure: dict[str, Any] = {}
AGGREGATIONS = {"average": "Average", "total": "Total", "maximum": "Maximum", "minimum": "Minimum", "count": "Count"}


def _azure_credential():
    if "cred" not in _azure:
        from azure.identity import DefaultAzureCredential  # type: ignore
        _azure["cred"] = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    return _azure["cred"]


def metric_namespace(resource_id: str) -> str:
    """/subscriptions/../providers/Microsoft.Sql/servers/s1/databases/db1 -> Microsoft.Sql/servers/databases"""
    parts = resource_id.split("/providers/", 1)[-1].strip("/").split("/")
    return "/".join([parts[0], *parts[1::2]])


def _azure_metrics(cfg: dict, resource: str, metric: str, aggregation: str):
    """One resource's metric. Uses azure-monitor-querymetrics (current SDK, needs the region) and falls back to
    MetricsQueryClient from azure-monitor-query < 2.0."""
    kw = {"timespan": timedelta(minutes=15), "granularity": timedelta(minutes=1), "aggregations": [aggregation]}
    try:
        from azure.monitor.querymetrics import MetricsClient  # type: ignore
    except ImportError:
        from azure.monitor.query import MetricsQueryClient  # type: ignore  # raises ImportError if neither
        client = _azure.setdefault("legacy", MetricsQueryClient(_azure_credential()))
        return client.query_resource(resource, metric_names=[metric], **kw)
    region = get(cfg, "region")
    if not region:
        raise ValueError("set the resource's region (e.g. westeurope): the metrics API is regional")
    key = f"metrics:{region}"
    if key not in _azure:
        _azure[key] = MetricsClient(f"https://{region}.metrics.monitor.azure.com", _azure_credential())
    results = _azure[key].query_resources(resource_ids=[resource], metric_names=[metric],
                                          metric_namespace=get(cfg, "metric_namespace") or metric_namespace(resource),
                                          **kw)
    return results[0]


def _azure_logs():
    if "logs" not in _azure:
        from azure.monitor.query import LogsQueryClient  # type: ignore
        _azure["logs"] = LogsQueryClient(_azure_credential())
    return _azure["logs"]


def azure_monitor(node_id: str, cfg: dict) -> list[Result]:
    resource, metric = get(cfg, "resource_id"), get(cfg, "metric_name")
    workspace, kql = get(cfg, "workspace_id"), get(cfg, "kql")
    if not ((resource and metric) or (workspace and kql)):
        return []
    parts: list[Result] = []

    if resource and metric:
        agg = str(get(cfg, "aggregation", "average")).lower()
        try:
            start = time.monotonic()
            res = _azure_metrics(cfg, resource, metric, AGGREGATIONS.get(agg, "Average"))
            ms = (time.monotonic() - start) * 1000
        except ImportError:
            return [Result(True, "install azure-monitor-querymetrics and azure-identity on the agent host", warn=True)]
        except Exception as exc:
            return [Result(False, f"Azure metric {metric}: {err(exc)}")]
        latest = []
        for m in res.metrics:
            for series in m.timeseries:
                values = [v for v in (getattr(p, agg, None) for p in series.data) if v is not None]
                if values:
                    latest.append(values[-1])
        label = get(cfg, "value_label") or f"{metric} {agg}"
        if not latest:
            parts.append(Result(True, f"no {metric} data in the last 15 min", warn=True, latency_ms=ms))
        else:
            r = thresholds(max(latest), cfg, "value", label)
            r.latency_ms, r.metrics = ms, {label: max(latest)}
            parts.append(r)

    if workspace and kql:
        try:
            start = time.monotonic()
            resp = _azure_logs().query_workspace(workspace, kql, timespan=timedelta(hours=1),
                                                 server_timeout=int(TIMEOUT * 2))
            ms = (time.monotonic() - start) * 1000
        except ImportError:
            return parts + [Result(True, "install azure-monitor-query and azure-identity on the agent host", warn=True)]
        except Exception as exc:
            return parts + [Result(False, f"Log Analytics query: {err(exc)}")]
        tables = getattr(resp, "tables", None) or getattr(resp, "partial_data", None) or []
        rows = tables[0].rows if tables else []
        label = get(cfg, "kql_label", "query result")
        if not rows:
            parts.append(Result(True, f"{label}: no rows", warn=True, latency_ms=ms))
        else:
            value = float(rows[0][0] or 0)
            r = thresholds(value, cfg, "kql", label)
            r.latency_ms, r.metrics = ms, {label: value}
            if getattr(resp, "partial_error", None):
                r.warn, r.message = True, r.message + " (partial result)"
            parts.append(r)
    return parts


TYPES: dict[str, Any] = {"kubernetes": kubernetes, "cloudwatch": cloudwatch, "azure_monitor": azure_monitor}
DEFAULT_INTERVALS = {"cloudwatch": 60, "azure_monitor": 60}
