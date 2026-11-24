---
name: setup-mdjaada
description: Install the MDJaada agent on a Linux server, Kubernetes cluster or Docker host and connect it to MDJaada (app.mdjaada.com or self-hosted) so the user's systems get monitored. Use when the user asks to set up, install, connect, upgrade or troubleshoot MDJaada monitoring or the MDJaada agent.
---

# Set up MDJaada monitoring

MDJaada is flow observability for data and service pipelines. The user draws their system in MDJaada; the
**agent** runs inside their network, checks every part on a schedule, receives their OpenTelemetry, and reports
out to the MDJaada API. This skill installs the agent and connects it to the user's MDJaada account.

Sources of truth (public, Apache 2.0): https://github.com/madhubolla2205/mdjaada-agent — `README.md`,
`deploy/systemd/README.md`, `deploy/systemd/install-agent.sh`, `deploy/kubernetes/agent.yaml`, `agent/Dockerfile`.
If anything below disagrees with those files, the files win. Only use flags and settings that appear there; run
`bash install.sh --help` if unsure.

## Rules for the agent key (read first)

The agent key lets a server report to the user's account. Treat it as a password.

- **Never ask the user to paste the key into this chat**, and never echo, log, commit or write it to a file
  yourself. If the user pastes it anyway, tell them to revoke it in MDJaada (account menu, agent keys) and create
  a new one.
- Never pass the key as a command-line argument (it lands in shell history and the process list). The installer
  refuses to take it as an argument on purpose.
- Steps that need the key are **run by the user in their own terminal**, where they type it into a hidden prompt.
  You prepare the exact command; they run it; you verify afterwards.
- Database passwords and other node secrets also stay on the agent host, in `secrets.env` (see step 6).

## 1. Ask the user (one short message)

1. **Hosted or self-hosted MDJaada?**
   - Hosted: API URL is `https://app.mdjaada.com/api`. No account yet: sign up for the 14-day trial at
     https://app.mdjaada.com/#signup (no card) and confirm the email.
   - Self-hosted: API URL is `<PUBLIC_URL>/api` of their MDJaada server, as the agent will reach it.
2. **Where should the agent run?** It must be able to reach the systems they want watched (databases, Kafka,
   queues, APIs) and the MDJaada API. Pick the path:
   - **A. Linux server with systemd** (x86-64 or ARM64): recommended, no Docker or Python needed.
   - **B. Inside a Kubernetes cluster**: read-only RBAC; lists Services and records rollouts as changes.
   - **C. Docker** on a host without systemd.
   - **D. From source** (macOS or a dev laptop, Python 3.10+): for trying it out, runs in the foreground.
3. **Flow name**: the exact name of the flow in MDJaada the agent reports to (for example `My system`). The agent
   waits until a flow with exactly that name exists.
4. **Agent id**: a name for this agent. On hosted MDJaada ids are shared across accounts, so make it unique, for
   example `<company>-<4 random letters>` (the "Connect your systems" dialog suggests one). Suggest one.
5. **Check only assigned parts?** Recommended when the agent sits in a different network from other agents. Then
   in MDJaada the user sets **Checked by agent** = the agent id on the parts this agent should check.

Then tell them where to get the key:
- Hosted: in MDJaada click **Connect your systems** → **Create agent key** (the first key is created
  automatically right after sign-up). Keep it ready, don't paste it here.
- Self-hosted: `MDJAADA_API_KEY` in the MDJaada server's `.env`.

## 2. Pre-flight checks (you run these)

Run on the machine where the agent will run (or say what you can't check from here):

```bash
uname -sm                                  # Linux x86_64/aarch64 for path A
command -v systemctl && systemctl --version | head -1
curl -sS -o /dev/null -w '%{http_code}\n' "<API_URL>/"   # any HTTP status means the API is reachable
(ss -ltn 2>/dev/null || netstat -ltn) | grep ':4318 ' || echo "port 4318 free"
```

- No HTTP status (DNS failure, timeout): fix network/proxy/firewall first; the agent only needs outbound HTTPS.
- Port 4318 is the agent's OTLP receiver. If it's taken, set `MDJAADA_OTLP_PORT` (or `0` to disable) after install.
- Not Linux or no systemd: use path C or D.

## 3A. Linux server with systemd (recommended)

Download the installer (you can do this):

```bash
mkdir -p ~/mdjaada-install && cd ~/mdjaada-install
curl -fsSLO https://github.com/madhubolla2205/mdjaada-releases/releases/latest/download/install.sh
bash install.sh --help
```

Then give the user this command to run **in their own terminal** (it needs `sudo` and asks for the key in a
hidden prompt). Fill in the values from step 1; add `--assigned-only` only if they chose it:

```bash
cd ~/mdjaada-install
sudo bash install.sh --api <API_URL> --agent-id <AGENT_ID> --flow-name "<FLOW_NAME>" --assigned-only
```

The installer downloads the signed single-file binary, verifies its checksum (and its signature if `cosign` is
installed; `--require-signature` makes that mandatory), creates the `mdjaada-agent` user, writes
`/etc/mdjaada-agent/agent.env` (mode 640), and starts a hardened systemd service that starts on boot.

Unattended installs (CI, config management) only: the key can come from the environment instead of the prompt,
for example from a root-only file: `sudo --preserve-env=MDJAADA_API_KEY bash install.sh ...`. Don't set this up
by putting the key in a script or this chat.

Verify (you can run these; `journalctl` may need `sudo`):

```bash
systemctl is-active mdjaada-agent
journalctl -u mdjaada-agent -n 50 --no-pager
```

Healthy logs show `MDJaada agent <id> v<version> -> <api> flow=<n> every 30s`, then `checked N/M nodes`.
See **Troubleshooting** for anything else.

Where things are: `/etc/mdjaada-agent/agent.env` (connection), `/etc/mdjaada-agent/secrets.env` (node secrets),
`/var/lib/mdjaada-agent/` (buffer and local telemetry), `/opt/mdjaada-agent/` (the agent).

## 3B. Kubernetes cluster

Needs `kubectl` pointing at the right cluster (confirm `kubectl config current-context` with the user).

**Image:** use the published image `ghcr.io/madhubolla2205/mdjaada-agent:<version>` (amd64 and arm64; pin a
version rather than `latest`; `<version>-full` adds the AWS, Google Cloud, Azure and Snowflake SDKs). Find the
current version on https://github.com/madhubolla2205/mdjaada-agent/pkgs/container/mdjaada-agent. Clusters without
internet access: copy it into the user's own registry, or build it from the repo
(`docker build -t <registry>/mdjaada-agent:<version> agent`, then push; for minikube/kind,
`minikube image load` / `kind load docker-image`).

Get the manifest:

```bash
curl -fsSLO https://raw.githubusercontent.com/madhubolla2205/mdjaada-agent/main/deploy/kubernetes/agent.yaml
```

Apply the manifest with the user's settings. The Secret is applied with the placeholder `change-me`; the user
sets the real key in the next step. The manifest sets `MDJAADA_ASSIGNED_ONLY` to `"true"`; if the user wants the
agent to check all parts, add `-e '/MDJAADA_ASSIGNED_ONLY/d'` to the `sed`:

```bash
sed -e 's#https://mdjaada.example.com/api#<API_URL>#' \
    -e 's#value: "Kubernetes"#value: "<FLOW_NAME>"#' \
    -e 's#value: "k8s-agent" }#value: "<AGENT_ID>" }#' \
    -e 's#image: mdjaada-agent:local#image: ghcr.io/madhubolla2205/mdjaada-agent:<VERSION>#' \
    agent.yaml | kubectl apply -f -
```

`MDJAADA_K8S_NAMESPACES` is `all` in the manifest; change it to a comma list (for example `shop,payments`) if the
user wants fewer namespaces.

The user then sets the key **in their own terminal** (hidden prompt, nothing in shell history) and restarts:

```bash
read -rs -p "MDJaada agent key: " K; echo
kubectl -n mdjaada create secret generic mdjaada-agent --from-literal=MDJAADA_API_KEY="$K" \
  --dry-run=client -o yaml | kubectl apply -f -
unset K
kubectl -n mdjaada rollout restart deploy/mdjaada-agent
```

Verify:

```bash
kubectl -n mdjaada rollout status deploy/mdjaada-agent
kubectl -n mdjaada logs deploy/mdjaada-agent --tail=50
```

Warn the user: re-applying `agent.yaml` with its placeholder Secret later resets the Secret to `change-me`. Re-run the secret
command after any re-apply, or remove the Secret document from the manifest they keep.

Apps in the cluster send telemetry to `http://mdjaada-agent.mdjaada:4318`.

## 3C. Docker (no systemd)

Use the published image `ghcr.io/madhubolla2205/mdjaada-agent` (amd64 and arm64). Tags: `<version>` and
`latest`; `<version>-full` and `full` add the AWS, Google Cloud, Azure and Snowflake SDKs. Pin a version for
anything long-running. Without registry access, build it instead:
`docker build -t mdjaada-agent:local agent` from a clone of the repo (`--build-arg WITH_AWS=true` etc. for the SDKs).

```bash
docker pull ghcr.io/madhubolla2205/mdjaada-agent:latest
```

The user runs this **in their own terminal**. `-e MDJAADA_API_KEY` with no value passes the key from the shell
without putting it on the command line:

```bash
read -rs -p "MDJaada agent key: " MDJAADA_API_KEY; echo; export MDJAADA_API_KEY
docker run -d --name mdjaada-agent --restart unless-stopped \
  -e MDJAADA_API=<API_URL> -e MDJAADA_API_KEY \
  -e MDJAADA_AGENT_ID=<AGENT_ID> -e MDJAADA_FLOW_NAME="<FLOW_NAME>" \
  -e MDJAADA_ASSIGNED_ONLY=true \
  -p 4318:4318 -v mdjaada-agent-data:/data ghcr.io/madhubolla2205/mdjaada-agent:latest
unset MDJAADA_API_KEY
```

Leave out `MDJAADA_ASSIGNED_ONLY` if they want it to check all parts. Node secrets for `env:NAME` settings go in
an env file passed with `--env-file` (mode 600, never committed). Verify with `docker logs --tail 50 mdjaada-agent`.

## 3D. From source (try it out, macOS or Linux)

Python 3.10+; the core needs only the standard library. Optional drivers: `psycopg[binary]`, `PyMySQL`,
`pymongo`, `confluent-kafka`, `PyYAML`.

```bash
git clone https://github.com/madhubolla2205/mdjaada-agent.git && cd mdjaada-agent
python3 -m venv .venv && . .venv/bin/activate
pip install "psycopg[binary]>=3.1" "PyMySQL>=1.1" "PyYAML>=6"    # only the drivers they need
```

The user runs it in their own terminal (foreground; Ctrl-C stops it):

```bash
read -rs -p "MDJaada agent key: " MDJAADA_API_KEY; echo; export MDJAADA_API_KEY
MDJAADA_API=<API_URL> MDJAADA_AGENT_ID=<AGENT_ID> MDJAADA_FLOW_NAME="<FLOW_NAME>" python agent/agent.py
```

For anything long-running, move to path A, B or C.

## 4. Connect it in MDJaada

1. Open the flow named `<FLOW_NAME>` (create it if it doesn't exist, the agent waits for it). Draw the system or
   start from a template: add a database, a queue or an API and connect them.
2. With `--assigned-only` / `MDJAADA_ASSIGNED_ONLY=true`, set **Checked by agent** = `<AGENT_ID>` on each part this
   agent should check.
3. Every part shows its status within a minute or two. Kubernetes agents also list Services under
   **Build › Discovered**.
4. Suggest a test: stop a test database or pause a consumer. MDJaada opens one incident, marks the likely cause
   and shows the evidence.

## 5. Send spans, logs and metrics (optional)

The agent receives **OTLP/HTTP with JSON encoding** on port 4318 (`/v1/traces`, `/v1/logs`, `/v1/metrics`).
For each app:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://<agent-host>:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/json
OTEL_SERVICE_NAME=orders-api      # match the part's name in MDJaada, or its `service` setting
```

gRPC/protobuf sources need an OpenTelemetry Collector in front:
`deploy/monitoring/otel-collector.yaml` in the repo. Telemetry stays on the agent host
(`MDJAADA_RETENTION_HOURS`, default 72) and is only queried during incidents.

## 6. Node secrets

In MDJaada, write a secret setting as `env:NAME` (for example a Postgres DSN `env:ORDERS_PG_DSN`). The agent
reads `NAME` from its own environment, so the platform only stores the reference.

- systemd: the user adds `NAME=value` to `/etc/mdjaada-agent/secrets.env`, then
  `sudo systemctl restart mdjaada-agent`. Let the user edit this file themselves (`sudo -e`); don't ask for the values.
- Kubernetes: add the variable to the `mdjaada-agent` Secret (same `read -rs` pattern as above) and restart.
- Docker: `--env-file` as above.

Kubernetes checks from a systemd agent outside the cluster: put a kubeconfig in `/etc/mdjaada-agent/` and set
`KUBECONFIG` to its path in `secrets.env`.

## Day to day

```bash
systemctl status mdjaada-agent
journalctl -u mdjaada-agent -f
sudo bash install.sh --upgrade              # newer version, settings kept (use the latest install.sh)
sudo bash install.sh --uninstall            # add --purge to also delete settings and data
```

Other settings (edit `/etc/mdjaada-agent/agent.env` or the container env, then restart): `MDJAADA_INTERVAL` (30),
`MDJAADA_OTLP_PORT` (4318, `0` disables), `MDJAADA_RETENTION_HOURS` (72), `MDJAADA_SLOW_SPAN_MS` (500),
`MDJAADA_HEALTHY_SAMPLE_RATE` (0.1), `MDJAADA_CHECK_TIMEOUT` (5), `MDJAADA_K8S_DISCOVERY`,
`MDJAADA_K8S_NAMESPACES`. The full list is in the repo README under Configuration.

## Troubleshooting

| Symptom | What to do |
|---|---|
| Log says `waiting for flow '<name>' to be created` | No flow with exactly that name. Create it in MDJaada, or fix `MDJAADA_FLOW_NAME` in `agent.env` and restart. |
| Log says `waiting for platform (...)` or `platform unreachable (...)` | Wrong `MDJAADA_API`, network/proxy blocking outbound HTTPS, or a wrong/revoked key (look for 401/403 in the message). Check the URL ends in `/api`, re-run the pre-flight `curl`, and create a new key if needed. Results are buffered on disk meanwhile. |
| 403 from app.mdjaada.com on an old agent | Agents before 0.4.2 can be blocked by bot protection; upgrade (`sudo bash install.sh --upgrade`). |
| Installer: `the binary does not run on this server` | Very old glibc (needs RHEL 8+/Ubuntu 20.04+/Debian 10+) or an unsupported CPU; use the source install from the repo (`deploy/systemd/install-agent.sh --source`) or Docker. |
| Installer: `checksum mismatch` | The download is corrupt or was altered. Download again; don't bypass it. |
| Installer: `could not create a virtual environment` (source install) | Debian/Ubuntu: `sudo apt install python3-venv`. |
| A part shows `degraded: install …` | A driver is missing for that type. Binary includes Postgres, MySQL, MongoDB, Kafka and YAML; AWS, GCP, Azure and Snowflake need the source install with `--drivers` or a Docker build arg. |
| Parts stay grey with `--assigned-only` | Set **Checked by agent** = the agent id on them. |
| Search in MDJaada says the agent is too old | Upgrade the agent. |

If it's still stuck, collect the last 50 log lines (remove anything secret) and point the user to
https://github.com/madhubolla2205/mdjaada-agent/issues/new/choose or hello@mdjaada.com.
