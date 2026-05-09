#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

py_mod() {
  local mod="$1"
  if command -v uv >/dev/null 2>&1; then
    uv sync --extra dev >/dev/null
    uv run python -m "${mod}"
  else
    export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"
    python3 -m "${mod}"
  fi
}

echo "Running FIRMS producer..."
py_mod wildfire_smoke.producers.firms_producer

echo "Running OpenAQ producer..."
py_mod wildfire_smoke.producers.openaq_producer

echo "Ingestion cycle complete."
