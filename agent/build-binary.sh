#!/usr/bin/env bash
# Build the single-file agent binary (dist/mdjaada-agent-linux-<arch>) with the Postgres, MySQL, MongoDB, Kafka and
# YAML drivers inside, so target servers need no Python. Run it in quay.io/pypa/manylinux_2_28_<arch> (glibc 2.28:
# RHEL/Alma/Rocky 8+, Ubuntu 20.04+, Debian 10+), from the agent/ folder:
#   docker run --rm -v "$PWD":/src -w /src/agent quay.io/pypa/manylinux_2_28_x86_64 ./build-binary.sh 0.3.0
# PyInstaller needs a shared libpython, which manylinux's own Pythons lack, so a portable CPython is fetched with uv.
set -euo pipefail
VERSION=${1:-0.0.0-dev}
case "$(uname -m)" in x86_64) ARCH=amd64 ;; aarch64) ARCH=arm64 ;; *) echo "unsupported arch $(uname -m)"; exit 1 ;; esac
OUT=dist/mdjaada-agent-linux-$ARCH
export UV_PYTHON_INSTALL_DIR=/tmp/uv-python UV_CACHE_DIR=/tmp/uv-cache

/opt/python/cp312-cp312/bin/python -m pip install --quiet --disable-pip-version-check "uv>=0.4"
UV=/opt/python/cp312-cp312/bin/uv
$UV python install 3.12
rm -rf /tmp/build-venv && $UV venv --quiet --python 3.12 /tmp/build-venv
$UV pip install --quiet --python /tmp/build-venv/bin/python "pyinstaller>=6.10,<7" \
  "psycopg[binary]>=3.1" "PyMySQL>=1.1" "pymongo>=4.8" "confluent-kafka>=2.5" "PyYAML>=6"

# the version shown by --version and in heartbeats
rm -rf /tmp/agent-src && mkdir -p /tmp/agent-src && cp -R mdjaada_agent agent.py /tmp/agent-src/
sed -i "s/^__version__ = .*/__version__ = \"$VERSION\"/" /tmp/agent-src/mdjaada_agent/__init__.py

/tmp/build-venv/bin/pyinstaller --onefile --clean --noconfirm --log-level WARN \
  --name "$(basename "$OUT")" --distpath dist --workpath /tmp/pyi-work --specpath /tmp/pyi-spec \
  --paths /tmp/agent-src --collect-submodules mdjaada_agent \
  --collect-all psycopg --collect-all psycopg_binary --collect-all confluent_kafka \
  --collect-submodules pymongo --collect-submodules bson --hidden-import pymysql --hidden-import yaml \
  /tmp/agent-src/agent.py

"$OUT" --version
"$OUT" --self-test
ls -la "$OUT"
