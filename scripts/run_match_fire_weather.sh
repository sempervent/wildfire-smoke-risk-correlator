#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"

COMPUTE_ENV=(
  -e PYTHONPATH=/app/src
  -e PSYCOPG_CONNINFO="host=postgres port=5432 dbname=${POSTGRES_DB:-smoke} user=${POSTGRES_USER:-smoke} password=${POSTGRES_PASSWORD:-smoke}"
  -e FIRE_WEATHER_MATCH_RADIUS_KM="${FIRE_WEATHER_MATCH_RADIUS_KM:-50}"
  -e FIRE_WEATHER_MATCH_MAX_TIME_DELTA_HOURS="${FIRE_WEATHER_MATCH_MAX_TIME_DELTA_HOURS:-3}"
  -e FIRE_WEATHER_MATCH_METHOD="${FIRE_WEATHER_MATCH_METHOD:-nearest_grid_cell}"
  -e SMOKE_RISK_LOOKBACK_HOURS="${SMOKE_RISK_LOOKBACK_HOURS:-24}"
)

echo "Python: match fires to gridded weather..."
${COMPOSE} exec -T \
  "${COMPUTE_ENV[@]}" \
  spark-master python3 /app/src/wildfire_smoke/spark/match_fire_weather.py
