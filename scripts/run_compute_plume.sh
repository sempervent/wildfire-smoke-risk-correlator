#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"

COMPUTE_ENV=(
  -e PYTHONPATH=/app/src
  -e PSYCOPG_CONNINFO="host=postgres port=5432 dbname=${POSTGRES_DB:-smoke} user=${POSTGRES_USER:-smoke} password=${POSTGRES_PASSWORD:-smoke}"
  -e WIND_MATCH_RADIUS_KM="${WIND_MATCH_RADIUS_KM:-100}"
  -e WIND_MATCH_LOOKBACK_HOURS="${WIND_MATCH_LOOKBACK_HOURS:-6}"
  -e PLUME_MAX_DISTANCE_KM="${PLUME_MAX_DISTANCE_KM:-150}"
  -e PLUME_HALF_ANGLE_DEGREES="${PLUME_HALF_ANGLE_DEGREES:-30}"
  -e SMOKE_RISK_LOOKBACK_HOURS="${SMOKE_RISK_LOOKBACK_HOURS:-24}"
  -e PLUME_MODEL_VERSION="${PLUME_MODEL_VERSION:-wind_v1}"
  -e PLUME_GRID_FALLBACK_TO_STATION="${PLUME_GRID_FALLBACK_TO_STATION:-1}"
  -e FIRE_WEATHER_MATCH_METHOD="${FIRE_WEATHER_MATCH_METHOD:-nearest_grid_cell}"
)

echo "Python: compute wind corridor plume exposures (PLUME_MODEL_VERSION=${PLUME_MODEL_VERSION:-wind_v1})..."
${COMPOSE} exec -T \
  "${COMPUTE_ENV[@]}" \
  spark-master python3 /app/src/wildfire_smoke/spark/compute_plume_exposure.py

echo "Plume computation complete."
