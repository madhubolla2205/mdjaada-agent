# MDJaada agent as a systemd service (no Docker)

For servers where you'd rather not run Docker, the agent runs as a hardened systemd service with its own user. It
needs outbound access to your MDJaada server and to the systems it checks. Two ways to install the same service:

- **Binary (no Python needed):** the signed single-file binary from
  https://github.com/madhubolla2205/mdjaada-releases (Postgres, MySQL, MongoDB, Kafka and YAML drivers inside):
  ```bash
  curl -fsSLO https://github.com/madhubolla2205/mdjaada-releases/releases/latest/download/install.sh
  sudo bash install.sh --api https://mdjaada.internal:8080/api --agent-id dc1-agent --assigned-only
  ```
- **Python (from this repository, below):** Python 3.10+, drivers installed with pip; also covers the AWS,
  Google Cloud, Azure and Snowflake SDKs.

## Install from source (Python)

Clone this repository on the server, then:

```bash
sudo ./deploy/systemd/install-agent.sh --api https://mdjaada.internal:8080/api --agent-id dc1-agent --assigned-only
```

It asks for the agent's API key (shown in MDJaada when you add an agent).

- `--agent-id`: in the UI, set **Checked by agent** to this id on the nodes this server should check.
- `--assigned-only`: check only those nodes (recommended for an agent in another network).
- `--drivers`: database/queue drivers to install with pip, default `postgres,mysql,mongodb,kafka,yaml`; add
  `aws`, `gcp`, `azure`, `snowflake`, or use `none` (HTTP, proxies, Redis, ClickHouse, Elasticsearch, RabbitMQ,
  Airflow, MLflow and Kubernetes need no driver). Without internet, set `PIP_INDEX_URL` to your mirror.
- Debian/Ubuntu: if creating the Python environment fails, `sudo apt install python3-venv`.
- The key is never taken as an argument (it would land in shell history and the process list). For unattended
  installs, export `MDJAADA_API_KEY` from a file instead: `sudo --preserve-env=MDJAADA_API_KEY ./systemd/install-agent.sh ...`

## Where things are

| | |
|---|---|
| `/etc/mdjaada-agent/agent.env` | connection settings (API, key, agent id, flow); root-owned, mode 640 |
| `/etc/mdjaada-agent/secrets.env` | secrets for node settings written as `env:NAME`; never overwritten by upgrades |
| `/var/lib/mdjaada-agent/` | buffered results and the local telemetry store (spans, logs, OTel metrics) |
| `/opt/mdjaada-agent/` | agent code and its Python environment |

After editing either env file: `sudo systemctl restart mdjaada-agent`.

## Day to day

```bash
systemctl status mdjaada-agent
journalctl -u mdjaada-agent -f
sudo ./deploy/systemd/install-agent.sh --upgrade        # newer version, settings kept (binary installs download it)
sudo ./deploy/systemd/install-agent.sh --uninstall      # --purge also deletes settings and data
```

The service restarts on failure and starts on boot. It runs as the `mdjaada-agent` user with no capabilities, a
read-only view of the system (`ProtectSystem=strict`), no access to home directories, and write access only to
`/var/lib/mdjaada-agent`. Apps can send OpenTelemetry to this server on port 4318 as with the Docker agent.
Kubernetes checks from outside a cluster: put a kubeconfig in `/etc/mdjaada-agent/` and set `KUBECONFIG` to its path
in `secrets.env`.
