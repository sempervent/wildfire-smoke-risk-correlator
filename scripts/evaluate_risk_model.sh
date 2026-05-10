#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if command -v uv >/dev/null 2>&1; then
  uv sync --extra dev >/dev/null
  uv run python -m wildfire_smoke.evaluate_risk "$@"
else
  export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"
  python3 -m wildfire_smoke.evaluate_risk "$@"
fi
