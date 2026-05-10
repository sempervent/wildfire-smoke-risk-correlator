#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export FIXTURE_TIME_MODE="${FIXTURE_TIME_MODE:-relative}"
export USE_ALIGNED_FIXTURES="${USE_ALIGNED_FIXTURES:-1}"
export FIRMS_DRY_RUN="${FIRMS_DRY_RUN:-1}"
export OPENAQ_DRY_RUN="${OPENAQ_DRY_RUN:-1}"
export WIND_DRY_RUN="${WIND_DRY_RUN:-1}"
export GRID_WEATHER_ENABLED="${GRID_WEATHER_ENABLED:-1}"
export GRID_WEATHER_DRY_RUN="${GRID_WEATHER_DRY_RUN:-1}"
export DISPERSION_ENABLED="${DISPERSION_ENABLED:-1}"
export PLUME_MODEL_VERSION="${PLUME_MODEL_VERSION:-wind_grid_v2}"
export PLUME_GRID_FALLBACK_TO_STATION="${PLUME_GRID_FALLBACK_TO_STATION:-1}"
export RISK_MODEL_VERSION="${RISK_MODEL_VERSION:-v5}"
export SMOKE_RISK_MODEL_VERSION="${SMOKE_RISK_MODEL_VERSION:-v5}"

echo "==> calibration-demo (fixtures → pipeline → observation load → compare → evaluate → summary)"

export REPLAY_RUN_PLUME=0
export REPLAY_RUN_COMPUTE=0
bash "${ROOT_DIR}/scripts/replay_fixtures.sh"

bash "${ROOT_DIR}/scripts/run_normalize.sh"
bash "${ROOT_DIR}/scripts/run_normalize_wind.sh"

bash "${ROOT_DIR}/scripts/replay_grid_weather_fixtures.sh"
bash "${ROOT_DIR}/scripts/run_normalize_grid_weather.sh"
bash "${ROOT_DIR}/scripts/run_match_fire_weather.sh"

bash "${ROOT_DIR}/scripts/run_compute_plume.sh"
bash "${ROOT_DIR}/scripts/run_compute_dispersion.sh"
bash "${ROOT_DIR}/scripts/run_compute_risk.sh"

bash "${ROOT_DIR}/scripts/run_compare_dispersion_aq.sh"

bash "${ROOT_DIR}/scripts/load_risk_observation_fixtures.sh"

export RISK_EVAL_MODEL_VERSION="${RISK_EVAL_MODEL_VERSION:-v5}"
bash "${ROOT_DIR}/scripts/evaluate_risk_model.sh"

bash "${ROOT_DIR}/scripts/calibration_summary.sh"

echo "calibration-demo complete."
