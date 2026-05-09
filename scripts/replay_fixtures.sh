#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export FIRMS_DRY_RUN="${FIRMS_DRY_RUN:-1}"
export OPENAQ_DRY_RUN="${OPENAQ_DRY_RUN:-1}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

REPLAY_RUN_NORMALIZE="${REPLAY_RUN_NORMALIZE:-1}"
REPLAY_RUN_COMPUTE="${REPLAY_RUN_COMPUTE:-1}"

echo "==> Replay fixtures to Kafka (no live API keys; uses FIRMS_DRY_RUN / OPENAQ_DRY_RUN)"
py_mod() {
  local mod="$1"
  if command -v uv >/dev/null 2>&1; then
    uv sync --extra dev >/dev/null
    uv run python -m "${mod}" "$@"
  else
    export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"
    python3 -m "${mod}" "$@"
  fi
}
py_mod wildfire_smoke.producers.firms_producer
py_mod wildfire_smoke.producers.openaq_producer

if [[ "${REPLAY_RUN_NORMALIZE}" == "1" ]]; then
  echo "==> Normalizing Kafka streams -> PostGIS (Spark)"
  bash "${ROOT_DIR}/scripts/run_normalize.sh"
else
  echo "Skipping normalization (REPLAY_RUN_NORMALIZE!=1)."
fi

if [[ "${REPLAY_RUN_COMPUTE}" == "1" ]]; then
  echo "==> Computing smoke risk scores"
  bash "${ROOT_DIR}/scripts/run_compute_risk.sh"
else
  echo "Skipping risk computation (REPLAY_RUN_COMPUTE!=1)."
fi

echo "Fixture replay complete."
