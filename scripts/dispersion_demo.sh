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

echo "==> Dispersion demo (aligned fixtures → normalize → grid → plume → dispersion → risk v5 → AQ compare)"

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

COMPOSE="${COMPOSE:-docker compose}"
echo "==> Summary"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-smoke}" -d "${POSTGRES_DB:-smoke}" -c \
  "SELECT * FROM analytics.v_dispersion_operational_summary;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-smoke}" -d "${POSTGRES_DB:-smoke}" -c \
  "SELECT geography_type, geoid, dispersion_score FROM analytics.v_top_dispersion_exposures LIMIT 15;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-smoke}" -d "${POSTGRES_DB:-smoke}" -c \
  "SELECT geography_type, geoid, lag_bucket, comparison_score, aq_observation_count FROM analytics.v_dispersion_aq_comparisons LIMIT 15;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-smoke}" -d "${POSTGRES_DB:-smoke}" -c \
  "SELECT geography_type, geoid, risk_score, risk_band FROM analytics.v_latest_smoke_risk_v5 LIMIT 15;"

echo "Dispersion demo complete."
