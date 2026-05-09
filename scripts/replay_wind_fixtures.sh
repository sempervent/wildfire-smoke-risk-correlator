#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export WIND_DRY_RUN="${WIND_DRY_RUN:-1}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

echo "==> Replay wind fixture to Kafka (WIND_DRY_RUN=${WIND_DRY_RUN})"
if command -v uv >/dev/null 2>&1; then
  uv sync --extra dev >/dev/null
  uv run python -m wildfire_smoke.producers.wind_producer
else
  PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}" python3 -m wildfire_smoke.producers.wind_producer
fi

echo "==> Normalize wind topic only"
bash "${ROOT_DIR}/scripts/run_normalize_wind.sh"

echo "Wind fixture replay complete."
