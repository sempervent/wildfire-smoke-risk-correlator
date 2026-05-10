#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if command -v uv >/dev/null 2>&1; then
  uv sync --extra dev >/dev/null
  exec uv run python -m wildfire_smoke.alerts send "$@"
fi
export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"
exec python3 -m wildfire_smoke.alerts send "$@"
