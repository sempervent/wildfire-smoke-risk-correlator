#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

py_mod() {
  local mod="$1"
  shift
  if command -v uv >/dev/null 2>&1; then
    uv sync --extra dev >/dev/null
    uv run python -m "${mod}" "$@"
  else
    export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"
    python3 -m "${mod}" "$@"
  fi
}

echo "==> Publishing malformed fixtures to raw Kafka topics (no API keys)"
py_mod wildfire_smoke.replay_bad_fixtures "$@"
