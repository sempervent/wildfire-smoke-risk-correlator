#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export GRID_WEATHER_ENABLED="${GRID_WEATHER_ENABLED:-1}"
export GRID_WEATHER_DRY_RUN="${GRID_WEATHER_DRY_RUN:-1}"
export PLUME_MODEL_VERSION="${PLUME_MODEL_VERSION:-wind_grid_v2}"
export PLUME_GRID_FALLBACK_TO_STATION="${PLUME_GRID_FALLBACK_TO_STATION:-1}"
export RISK_MODEL_VERSION="${RISK_MODEL_VERSION:-v4}"

echo "==> Grid weather demo (fixture → normalize → match → plume v2 → risk v4)"

bash "${ROOT_DIR}/scripts/replay_grid_weather_fixtures.sh"
bash "${ROOT_DIR}/scripts/run_normalize_grid_weather.sh"
bash "${ROOT_DIR}/scripts/run_match_fire_weather.sh"
bash "${ROOT_DIR}/scripts/run_compute_plume.sh"
bash "${ROOT_DIR}/scripts/run_compute_risk.sh"

COMPOSE="${COMPOSE:-docker compose}"
echo "==> Summary"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-smoke}" -d "${POSTGRES_DB:-smoke}" -c \
  "SELECT * FROM analytics.v_grid_weather_operational_summary;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-smoke}" -d "${POSTGRES_DB:-smoke}" -c \
  "SELECT match_method, match_rows, distinct_fires FROM analytics.v_fire_weather_match_summary;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-smoke}" -d "${POSTGRES_DB:-smoke}" -c \
  "SELECT geography_type, geoid, risk_score, risk_band FROM analytics.v_latest_smoke_risk_v4 LIMIT 15;"

echo "Grid weather demo complete."
