# MDJaada agent

*Jaada* (Urdu/Persian): road, route, pathway. MDJaada follows the path a failure takes through your system back to
where it started.

This is the open-source **agent** of [MDJaada](https://mdjaada.com), drag-and-drop flow observability for data and
service pipelines. You draw your system in MDJaada (databases, Kafka, queues, APIs, Airflow DAGs, ML models), and the
agent runs **inside your network**, checks every node on a schedule, receives your OpenTelemetry spans, logs and
metrics, and keeps that telemetry locally. When something breaks, MDJaada opens one incident, ranks the likely causes
and pulls the matching spans and log lines from the agent as evidence.

The agent's code is public so you can read exactly what runs in your environment and what it sends out.

| Canvas | Incident |
|---|---|
| ![Canvas](docs/images/canvas-light.jpg) | ![Incident](docs/images/incident-light.jpg) |

**Try MDJaada:** a 14-day trial at [app.mdjaada.com](https://app.mdjaada.com), then free for good for up to 10 nodes
and 1 agent; larger systems pay per node per day with no per-user fees ([pricing](https://mdjaada.com/#pricing)). Or ask
for a self-hosted pilot at [mdjaada.com](https://mdjaada.com). The agent needs an MDJaada account (hosted or
self-hosted) to report to.

## Quick start (about 10 minutes)

1. **Sign up** for the free 14-day trial at [app.mdjaada.com](https://app.mdjaada.com/#signup) (no card) and confirm
   your email. Your agent key and an install command filled in for your account appear straight away.
2. **Install the agent** on a Linux server that can reach the systems you want to watch:
   ```bash
   curl -fsSLO https://github.com/madhubolla2205/mdjaada-releases/releases/latest/download/install.sh
   sudo bash install.sh --api https://app.mdjaada.com/api --agent-id dc1-agent --flow-name "My system"
   ```
   It asks for the agent key, so the key never lands in your shell history.
3. **Draw your system** in MDJaada, or start from a template: add a database, a queue or an API and connect them.
   Every part shows its status within a minute or two.
4. **Break something on purpose**, such as stopping a test database or pausing a consumer. MDJaada opens one incident,
   marks the likely cause and shows the evidence.

Prefer to watch first? The [3-minute walkthrough](https://mdjaada.com/#videos) goes from sign-up to the first incident.

## Set up with Claude Code

If you use [Claude Code](https://claude.com/claude-code), it can install and connect the agent for you. Add this
repository as a plugin marketplace and install the `mdjaada` plugin:

```
/plugin marketplace add madhubolla2205/mdjaada-agent
/plugin install mdjaada@mdjaada
```

Then ask Claude to "set up MDJaada monitoring". It asks whether you use app.mdjaada.com or a self-hosted MDJaada and
where the agent should run (a Linux server, a Kubernetes cluster, Docker, or from source), checks the machine, and
prepares the install. Claude never asks for your agent key: the steps that need it run in your own terminal, where you
type it into a hidden prompt. The skill is in [claude-plugin/skills/setup-mdjaada](claude-plugin/skills/setup-mdjaada/SKILL.md).

## What stays in your network

- **Telemetry stays with the agent.** Spans, logs and OTel metric points are stored on the agent host
  (`MDJAADA_RETENTION_HOURS`, default 72) and only queried on demand during an incident.
- **Secrets stay on the agent host.** Write any node setting as `env:VAR_NAME` (for example
  `"password": "env:MYSQL_PASSWORD"`) and the agent reads it from its own environment. See
  [examples/secrets.env.example](examples/secrets.env.example).
- **No inbound access from MDJaada is needed.** The agent connects out to your MDJaada API and to the systems it
  checks; it listens only on port 4318 for your own apps' telemetry. The platform keeps flow config, health status,
  change events and incident summaries.

## Install

**Linux binary as a systemd service** (no Python needed; signed releases in
[mdjaada-releases](https://github.com/madhubolla2205/mdjaada-releases)):

```bash
curl -fsSLO https://github.com/madhubolla2205/mdjaada-releases/releases/latest/download/install.sh
sudo bash install.sh --api https://app.mdjaada.com/api --agent-id dc1-agent --assigned-only
```

**Docker:** images for amd64 and arm64 are on GitHub Container Registry:

```bash
docker run -d --name mdjaada-agent --restart unless-stopped \
  -e MDJAADA_API=https://app.mdjaada.com/api -e MDJAADA_API_KEY -e MDJAADA_AGENT_ID=dc1-agent \
  -e MDJAADA_FLOW_NAME="My system" -p 4318:4318 -v mdjaada-agent-data:/data \
  ghcr.io/madhubolla2205/mdjaada-agent:latest
```

`-e MDJAADA_API_KEY` with no value passes the key from your shell (`read -rs MDJAADA_API_KEY; export MDJAADA_API_KEY`)
so it stays off the command line. Tags: `<version>` and `latest`, plus `<version>-full` and `full` with the AWS,
Google Cloud, Azure and Snowflake SDKs. Or build your own with `docker build agent` (add `--build-arg
WITH_AWS=true`, `WITH_GCP`, `WITH_AZURE`, `WITH_SNOWFLAKE` for the cloud connectors).

**Kubernetes:** [deploy/kubernetes/agent.yaml](deploy/kubernetes/agent.yaml) (read-only RBAC, non-root, read-only
filesystem). Set its `image:` to `ghcr.io/madhubolla2205/mdjaada-agent:<version>` or your own build.

**From source:** Python 3.10+, standard library only for the core: `python agent/agent.py`. Database drivers are
optional (`psycopg`, `PyMySQL`, `pymongo`, `confluent-kafka`, `PyYAML`). Systemd details are in
[deploy/systemd/README.md](deploy/systemd/README.md).

## What it checks

Every node accepts the generic checks (`host`+`port` TCP, `health_url`, `metrics_url`+`metric`) plus:

| Area | Node types |
|---|---|
| Services | HTTP service, proxy, custom (SQL, Prometheus metric, URL or port) |
| Databases and stores | Postgres, MySQL / MariaDB, ClickHouse, Snowflake, MongoDB, Redis, Elasticsearch / OpenSearch |
| Streams and queues | Kafka cluster / topic / consumer group, RabbitMQ cluster / queue, AWS SQS, Google Pub/Sub |
| Pipelines and ML | Airflow DAG (2.x and 3.x), MLflow model |
| Platforms | Kubernetes workloads (rollouts recorded as deploy events), AWS CloudWatch, Azure Monitor |

Queues and streams work as layers: the agent discovers each cluster's topics, consumer groups and queues and
suggests them in MDJaada, so a missing broker, a topic nothing is produced to and a stalled consumer are each reported
on the right node. A missing driver shows the node as *degraded: install …*, never as down.

## Sending spans, logs and metrics

The agent's receiver speaks **OTLP/HTTP with JSON encoding** on port 4318 (`/v1/traces`, `/v1/logs`, `/v1/metrics`):

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://<agent-host>:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/json
OTEL_SERVICE_NAME=orders-api        # match the node name or the node's `service` setting
```

For gRPC/protobuf sources, put an OpenTelemetry Collector in front:
[deploy/monitoring/otel-collector.yaml](deploy/monitoring/otel-collector.yaml).

All error spans and spans slower than `MDJAADA_SLOW_SPAN_MS` are kept; healthy spans are sampled per trace at
`MDJAADA_HEALTHY_SAMPLE_RATE`.

## Configuration

`MDJAADA_API`, `MDJAADA_API_KEY`, `MDJAADA_FLOW_ID` or `MDJAADA_FLOW_NAME`, `MDJAADA_AGENT_ID`,
`MDJAADA_ASSIGNED_ONLY`, `MDJAADA_INTERVAL` (30), `MDJAADA_TASK_POLL` (5), `MDJAADA_OTLP_PORT` (4318, `0` disables),
`MDJAADA_DATA_DIR`, `MDJAADA_RETENTION_HOURS` (72), `MDJAADA_METRICS_RETENTION_HOURS` (6), `MDJAADA_BUFFER_MB` (50),
`MDJAADA_SLOW_SPAN_MS` (500), `MDJAADA_HEALTHY_SAMPLE_RATE` (0.1), `MDJAADA_CHECK_TIMEOUT` (5),
`MDJAADA_K8S_DISCOVERY`, `MDJAADA_K8S_NAMESPACES`.

## Development

```bash
pip install pytest pyyaml
pytest -q agent/tests
```

Issues and pull requests are welcome.

## Feedback

MDJaada is new, and feedback from people trying it on real systems is the most useful thing you can give. Open an
[issue](https://github.com/madhubolla2205/mdjaada-agent/issues/new/choose) for bugs and ideas, or email
[hello@mdjaada.com](mailto:hello@mdjaada.com?subject=MDJaada%20feedback) for anything about the web app, your account
or a self-hosted pilot.

## Licence

The agent is licensed under the [Apache License 2.0](LICENSE). The MDJaada platform (API, web app and hosted service)
is not part of this repository and is not open source.
