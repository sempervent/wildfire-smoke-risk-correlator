#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"
export DRY_RUN="${DRY_RUN:-1}"

py_mod() {
  if command -v uv >/dev/null 2>&1; then
    uv sync --extra dev >/dev/null
    uv run python -m wildfire_smoke.replay_dlq "$@"
  else
    export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"
    python3 -m wildfire_smoke.replay_dlq "$@"
  fi
}

echo "==> DLQ / parse-error replay (DRY_RUN=${DRY_RUN}; pass --no-dry-run or DRY_RUN=0 for writes)"
py_mod "$@"
