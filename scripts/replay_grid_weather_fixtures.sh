#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

# shellcheck source=scripts/lib/fixture_paths.sh
source "${ROOT_DIR}/scripts/lib/fixture_paths.sh"
apply_aligned_fixture_paths

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"
export GRID_WEATHER_DRY_RUN="${GRID_WEATHER_DRY_RUN:-1}"
export GRID_WEATHER_ENABLED="${GRID_WEATHER_ENABLED:-1}"

echo "==> Replay gridded weather fixture to Kafka (GRID_WEATHER_DRY_RUN=${GRID_WEATHER_DRY_RUN})"

py_mod() {
  if command -v uv >/dev/null 2>&1; then
    uv sync --extra dev >/dev/null
    uv run python -m wildfire_smoke.producers.grid_weather_producer "$@"
  else
    export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"
    python3 -m wildfire_smoke.producers.grid_weather_producer "$@"
  fi
}

py_mod "$@"
