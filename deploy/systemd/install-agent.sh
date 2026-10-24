#!/usr/bin/env bash
# Install (or upgrade) the MDJaada agent as a systemd service, without Docker.
#
#   sudo ./install-agent.sh --api https://mdjaada.internal:8080/api --agent-id dc1-agent [options]
#   sudo ./install-agent.sh --upgrade        newer agent, settings kept (same install kind as before)
#   sudo ./install-agent.sh --uninstall      stop and remove the service (add --purge to delete config and data)
#
# What gets installed (default: source when this script sits in the self-host package, otherwise download):
#   --download [--version X]  the single-file binary from the public releases (Postgres, MySQL, MongoDB, Kafka and
#                             YAML drivers inside, no Python needed); checksum verified, signature too if cosign is
#                             installed (--require-signature makes that mandatory)
#   --binary FILE             a binary you downloaded yourself
#   --source                  the Python agent from the package (Python 3.10+), drivers via pip:
#     --drivers LIST          postgres,mysql,mongodb,kafka,yaml (default), aws, gcp, azure, snowflake, all, none
#     --python PATH           Python to use (default python3)
#
# Settings:
#   --api URL            MDJaada API as the agent reaches it (hosted: https://app.mdjaada.com/api;
#                        self-host package: <PUBLIC_URL>/api)
#   --agent-id ID        this agent's name (with --assigned-only, set "Checked by agent" = ID on its nodes)
#   --flow-name NAME     flow to report to (default "My flow"), or --flow-id N
#   --assigned-only      check only nodes assigned to this agent (recommended for agents in other networks)
# The agent key is read from $MDJAADA_API_KEY or asked for (never pass it as an argument: it would end up in
# your shell history and the process list). Hosted: the agent key from MDJaada -> Connect your systems.
# Self-hosted: MDJAADA_API_KEY in the MDJaada server's .env.
set -euo pipefail

NAME=mdjaada-agent
APP=/opt/$NAME
CONF=/etc/$NAME
STATE=/var/lib/$NAME
UNIT=/etc/systemd/system/$NAME.service
HERE=$(cd "$(dirname "$0")" && pwd)
RELEASES=${MDJAADA_RELEASES_URL:-https://github.com/madhubolla2205/mdjaada-releases}
RELEASE_VERSION="__RELEASE_VERSION__"   # set when this script is published with a release
# who may sign releases: the release workflow of the MDJaada repository, on a version tag
SIGNER='^https://github\.com/madhubolla2205/mdjaada/\.github/workflows/release\.yml@refs/tags/v'
ISSUER=https://token.actions.githubusercontent.com

API="" AGENT_ID="" FLOW_NAME="" FLOW_ID="" ASSIGNED="" DRIVERS="postgres,mysql,mongodb,kafka,yaml" PY=python3
MODE=install PURGE="" KIND="" BINARY="" VERSION="" REQUIRE_SIG=""
die() { echo "error: $*" >&2; exit 1; }
say() { echo "==> $*"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --api) API=${2:?}; shift 2 ;;
    --agent-id) AGENT_ID=${2:?}; shift 2 ;;
    --flow-name) FLOW_NAME=${2:?}; shift 2 ;;
    --flow-id) FLOW_ID=${2:?}; shift 2 ;;
    --assigned-only) ASSIGNED=true; shift ;;
    --drivers) DRIVERS=${2:?}; shift 2 ;;
    --python) PY=${2:?}; shift 2 ;;
    --download) KIND=download; shift ;;
    --version) VERSION=${2:?}; KIND=download; shift 2 ;;
    --binary) BINARY=${2:?}; KIND=binary; shift 2 ;;
    --source) KIND=source; shift ;;
    --require-signature) REQUIRE_SIG=yes; shift ;;
    --upgrade) MODE=upgrade; shift ;;
    --uninstall) MODE=uninstall; shift ;;
    --purge) PURGE=yes; shift ;;
    -h|--help) sed -n '2,26p' "$0"; exit 0 ;;
    *) die "unknown option $1 (see --help)" ;;
  esac
done

[ "$(id -u)" = 0 ] || die "run as root (sudo)"
command -v systemctl >/dev/null || die "systemd not found; run the agent with Docker or 'python3 -m mdjaada_agent' instead"

if [ "$MODE" = uninstall ]; then
  systemctl disable --now "$NAME" 2>/dev/null || true
  rm -f "$UNIT" && systemctl daemon-reload
  rm -rf "$APP"
  if [ -n "$PURGE" ]; then
    rm -rf "$CONF" "$STATE"
    userdel "$NAME" 2>/dev/null || true
    say "removed the service, its settings and its data"
  else
    say "removed the service; settings kept in $CONF and data in $STATE (--purge deletes them)"
  fi
  exit 0
fi

# ---------------------------------------------------------------- what to install
SRC=""
for d in "$HERE/../../agent" "$HERE/../src/agent" "$HERE/../agent"; do
  [ -f "$d/mdjaada_agent/__main__.py" ] && SRC=$(cd "$d" && pwd) && break
done
if [ -z "$KIND" ]; then
  if [ "$MODE" = upgrade ] && [ -f "$APP/.install-kind" ]; then KIND=$(cat "$APP/.install-kind")
  elif [ -n "$SRC" ]; then KIND=source
  else KIND=download
  fi
fi
[ "$KIND" = source ] && [ -z "$SRC" ] && die "agent source not found next to this script (expected ../src/agent)"

if [ "$MODE" = install ] && [ ! -f "$CONF/agent.env" ]; then
  [ -n "$API" ] || die "--api is required for a new install"
  [ -n "$AGENT_ID" ] || die "--agent-id is required for a new install"
fi
[ "$MODE" = upgrade ] && [ ! -f "$CONF/agent.env" ] && die "nothing to upgrade: $CONF/agent.env not found"

KEY=${MDJAADA_API_KEY:-}
if [ ! -f "$CONF/agent.env" ] && [ -z "$KEY" ]; then
  if [ -t 0 ]; then read -r -s -p "MDJaada agent key: " KEY; echo
  elif [ -r /dev/tty ]; then read -r -s -p "MDJaada agent key: " KEY < /dev/tty; echo
  else die "set MDJAADA_API_KEY to your agent key (hosted: MDJaada -> Connect your systems; self-hosted: MDJAADA_API_KEY in the server's .env)"
  fi
  [ -n "$KEY" ] || die "an agent key is required"
fi

fetch() {  # url file
  if command -v curl >/dev/null; then curl -fsSL --retry 3 -o "$2" "$1"
  elif command -v wget >/dev/null; then wget -q -O "$2" "$1"
  else die "curl or wget is needed to download the agent"
  fi
}

download_binary() {  # -> sets BINARY to a verified file
  local arch tag base tmp file
  case "$(uname -m)" in x86_64|amd64) arch=amd64 ;; aarch64|arm64) arch=arm64 ;; *) die "no agent binary for $(uname -m); use --source" ;; esac
  tag=${VERSION:-}
  [ -z "$tag" ] && [ "$RELEASE_VERSION" != "__RELEASE_VERSION__" ] && tag=$RELEASE_VERSION
  if [ -n "$tag" ]; then tag=v${tag#v}; base="$RELEASES/releases/download/$tag"; else base="$RELEASES/releases/latest/download"; fi
  tmp=$(mktemp -d); file=mdjaada-agent-linux-$arch
  say "downloading $file (${tag:-latest}) from $RELEASES"
  fetch "$base/$file" "$tmp/$file" || die "download failed: $base/$file"
  fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS" || die "download failed: $base/SHA256SUMS"
  (cd "$tmp" && grep " $file\$" SHA256SUMS | sha256sum -c --quiet -) || die "checksum mismatch: the download is corrupt or was altered"
  say "checksum ok"
  if command -v cosign >/dev/null; then
    fetch "$base/SHA256SUMS.sigstore.json" "$tmp/SHA256SUMS.sigstore.json" || die "signature not found for this release"
    cosign verify-blob --bundle "$tmp/SHA256SUMS.sigstore.json" --certificate-identity-regexp "$SIGNER" \
      --certificate-oidc-issuer "$ISSUER" "$tmp/SHA256SUMS" >/dev/null 2>&1 \
      || die "signature check failed: this release was not signed by the MDJaada release workflow"
    say "signature ok (signed by the MDJaada release workflow)"
  elif [ -n "$REQUIRE_SIG" ]; then
    die "--require-signature needs cosign (https://docs.sigstore.dev/cosign/system_config/installation/)"
  else
    echo "    (install cosign to also verify the release signature)"
  fi
  BINARY="$tmp/$file"
}

# ---------------------------------------------------------------- user, files
id "$NAME" >/dev/null 2>&1 || useradd --system --home-dir "$STATE" --no-create-home --shell /usr/sbin/nologin "$NAME"
install -d -m 755 "$APP"
install -d -m 750 -o root -g "$NAME" "$CONF"

case "$KIND" in
  download|binary)
    [ "$KIND" = download ] && download_binary
    [ -f "$BINARY" ] || die "binary not found: $BINARY"
    install -d -m 755 "$APP/bin"
    install -m 755 "$BINARY" "$APP/bin/$NAME.new"   # executable copy first: downloads arrive without the x bit
    if ! "$APP/bin/$NAME.new" --self-test >/dev/null 2>&1; then
      "$APP/bin/$NAME.new" --self-test || true
      rm -f "$APP/bin/$NAME.new"
      die "the binary does not run on this server (see above)"
    fi
    mv "$APP/bin/$NAME.new" "$APP/bin/$NAME"
    rm -rf "$APP/venv" "$APP/mdjaada_agent"
    say "installed $("$APP/bin/$NAME" --version)"
    EXEC="$APP/bin/$NAME"
    ;;
  source)
    "$PY" -c 'import sys; sys.exit(sys.version_info < (3, 10))' 2>/dev/null \
      || die "$PY is missing or older than 3.10 (use --python /path/to/python3.10+, or the binary: --download)"
    say "copying agent code from $SRC"
    rm -rf "$APP/mdjaada_agent.new" && cp -R "$SRC/mdjaada_agent" "$APP/mdjaada_agent.new"
    find "$APP/mdjaada_agent.new" -name __pycache__ -prune -exec rm -rf {} +
    rm -rf "$APP/mdjaada_agent" && mv "$APP/mdjaada_agent.new" "$APP/mdjaada_agent"
    rm -rf "$APP/bin"
    if [ ! -x "$APP/venv/bin/python" ]; then
      say "creating Python environment in $APP/venv"
      "$PY" -m venv "$APP/venv" 2>/dev/null \
        || die "could not create a virtual environment; on Debian/Ubuntu: apt install python3-venv"
    fi
    pkgs=()
    IFS=',' read -r -a wanted <<< "$DRIVERS"
    for d in "${wanted[@]}"; do
      case "${d// /}" in
        all) pkgs+=("psycopg[binary]>=3.1" "PyMySQL>=1.1" "pymongo>=4.8" "confluent-kafka>=2.5" "PyYAML>=6" "boto3>=1.35"
                    "google-cloud-monitoring>=2.22" "azure-monitor-querymetrics>=1.0" "azure-monitor-query>=2.0"
                    "azure-identity>=1.19" "snowflake-connector-python>=3.12") ;;
        none|"") ;;
        postgres) pkgs+=("psycopg[binary]>=3.1") ;;
        mysql) pkgs+=("PyMySQL>=1.1") ;;
        mongodb) pkgs+=("pymongo>=4.8") ;;
        kafka) pkgs+=("confluent-kafka>=2.5") ;;
        yaml) pkgs+=("PyYAML>=6") ;;
        aws) pkgs+=("boto3>=1.35") ;;
        gcp) pkgs+=("google-cloud-monitoring>=2.22") ;;
        azure) pkgs+=("azure-monitor-querymetrics>=1.0" "azure-monitor-query>=2.0" "azure-identity>=1.19") ;;
        snowflake) pkgs+=("snowflake-connector-python>=3.12") ;;
        *) die "unknown driver '$d' (postgres, mysql, mongodb, kafka, yaml, aws, gcp, azure, snowflake, all, none)" ;;
      esac
    done
    if [ ${#pkgs[@]} -gt 0 ]; then
      say "installing drivers: ${pkgs[*]}"
      "$APP/venv/bin/pip" install --quiet --disable-pip-version-check --no-cache-dir --upgrade "${pkgs[@]}" \
        || die "driver install failed (no internet? set PIP_INDEX_URL to your mirror, or use --drivers none)"
    fi
    (cd "$APP" && "$APP/venv/bin/python" -m mdjaada_agent --version) || die "the agent does not start with $PY"
    EXEC="$APP/venv/bin/python -m mdjaada_agent"
    ;;
esac
# upgrades repeat the same kind: a binary install upgrades from the releases, a source install from the package
if [ "$KIND" = source ]; then echo source > "$APP/.install-kind"; else echo download > "$APP/.install-kind"; fi

# ---------------------------------------------------------------- settings (kept on upgrade)
set_key() {  # file key value: replace or append KEY=value
  local f=$1 k=$2 v=$3
  touch "$f"
  grep -v "^$k=" "$f" > "$f.tmp" || true
  printf '%s=%s\n' "$k" "$v" >> "$f.tmp"
  mv "$f.tmp" "$f"
}
ENV_FILE=$CONF/agent.env
if [ ! -f "$ENV_FILE" ]; then
  printf '# MDJaada agent connection settings (install-agent.sh). Restart after editing: systemctl restart %s\n' \
    "$NAME" > "$ENV_FILE"
  set_key "$ENV_FILE" MDJAADA_FLOW_NAME "${FLOW_NAME:-My flow}"
fi
[ -n "$API" ] && set_key "$ENV_FILE" MDJAADA_API "$API"
[ -n "$KEY" ] && set_key "$ENV_FILE" MDJAADA_API_KEY "$KEY"
[ -n "$AGENT_ID" ] && set_key "$ENV_FILE" MDJAADA_AGENT_ID "$AGENT_ID"
[ -n "$FLOW_NAME" ] && set_key "$ENV_FILE" MDJAADA_FLOW_NAME "$FLOW_NAME"
if [ -n "$FLOW_ID" ]; then
  set_key "$ENV_FILE" MDJAADA_FLOW_ID "$FLOW_ID"
  grep -v '^MDJAADA_FLOW_NAME=' "$ENV_FILE" > "$ENV_FILE.tmp"; mv "$ENV_FILE.tmp" "$ENV_FILE"
fi
[ -n "$ASSIGNED" ] && set_key "$ENV_FILE" MDJAADA_ASSIGNED_ONLY true
if [ ! -f "$CONF/secrets.env" ]; then
  cat > "$CONF/secrets.env" <<'EOF'
# Secrets for node settings. A setting written as env:NAME in the MDJaada UI is read from here on this server.
# Example: Postgres node DSN = env:ORDERS_PG_DSN
# ORDERS_PG_DSN=postgresql://monitor:secret@db.internal:5432/orders
# Kubernetes outside the cluster: put a kubeconfig in /etc/mdjaada-agent/ and set KUBECONFIG to its path
# Restart after editing: systemctl restart mdjaada-agent
EOF
fi
chown root:"$NAME" "$ENV_FILE" "$CONF/secrets.env"
chmod 640 "$ENV_FILE" "$CONF/secrets.env"
# the binary unpacks itself at start; keep that inside the agent's own directory (works where /tmp is noexec)
install -d -m 750 -o "$NAME" -g "$NAME" "$STATE" "$STATE/tmp"

# ---------------------------------------------------------------- service
cat > "$UNIT" <<EOF
# MDJaada agent (written by install-agent.sh; re-run it to change). status: systemctl status $NAME
# logs: journalctl -u $NAME -f
[Unit]
Description=MDJaada agent (checks, OTLP receiver)
Documentation=https://mdjaada.com/#install
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=$NAME
Group=$NAME
WorkingDirectory=$APP
# connection settings and your node secrets (env:NAME), root-owned, mode 640
EnvironmentFile=$CONF/agent.env
EnvironmentFile=-$CONF/secrets.env
Environment=MDJAADA_DATA_DIR=$STATE
Environment=HOME=$STATE
Environment=TMPDIR=$STATE/tmp
Environment=PYTHONUNBUFFERED=1
ExecStart=$EXEC
Restart=always
RestartSec=5
TimeoutStopSec=30
# buffered results and the local telemetry store (spans, logs, OTel metrics)
StateDirectory=$NAME
StateDirectoryMode=0750

# hardening: the agent only reads its code and config, writes its state directory, and talks over the network
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
ProtectClock=yes
ProtectHostname=yes
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
LockPersonality=yes
CapabilityBoundingSet=
AmbientCapabilities=
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX AF_NETLINK
SystemCallArchitectures=native
UMask=0027

[Install]
WantedBy=multi-user.target
EOF
chmod 644 "$UNIT"
systemctl daemon-reload
systemctl enable "$NAME" >/dev/null 2>&1
systemctl restart "$NAME"
sleep 3
if systemctl is-active --quiet "$NAME"; then
  id_shown=$(grep '^MDJAADA_AGENT_ID=' "$ENV_FILE" | cut -d= -f2-)
  say "mdjaada-agent is running as '$id_shown' and starts on boot"
  echo "    logs:     journalctl -u $NAME -f"
  echo "    secrets:  $CONF/secrets.env  (then: systemctl restart $NAME)"
  if [ -n "$ASSIGNED" ] || grep -q '^MDJAADA_ASSIGNED_ONLY=true' "$ENV_FILE"; then
    echo "    in MDJaada set \"Checked by agent\" = $id_shown on the nodes this server should check"
  fi
else
  journalctl -u "$NAME" -n 30 --no-pager || true
  die "the service did not stay up; see the log above"
fi
