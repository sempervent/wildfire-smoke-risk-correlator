#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"

COMPUTE_ENV=(
  -e PYTHONPATH=/app/src
  -e PSYCOPG_CONNINFO="host=postgres port=5432 dbname=${POSTGRES_DB:-smoke} user=${POSTGRES_USER:-smoke} password=${POSTGRES_PASSWORD:-smoke}"
  -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092
  -e SMOKE_RISK_MODEL_VERSION="${SMOKE_RISK_MODEL_VERSION:-v2}"
  -e RISK_MODEL_VERSION="${RISK_MODEL_VERSION:-}"
  -e PLUME_MODEL_VERSION="${PLUME_MODEL_VERSION:-wind_v1}"
  -e PLUME_GRID_FALLBACK_TO_STATION="${PLUME_GRID_FALLBACK_TO_STATION:-1}"
  -e SMOKE_RISK_LOOKBACK_HOURS="${SMOKE_RISK_LOOKBACK_HOURS:-24}"
  -e SMOKE_RISK_NEARBY_KM="${SMOKE_RISK_NEARBY_KM:-50}"
  -e SMOKE_RISK_GEOGRAPHIES="${SMOKE_RISK_GEOGRAPHIES:-both}"
  -e DISPERSION_MODEL_VERSION="${DISPERSION_MODEL_VERSION:-gaussian_v0}"
)

echo "Python: compute smoke risk..."
${COMPOSE} exec -T \
  "${COMPUTE_ENV[@]}" \
  spark-master python3 /app/src/wildfire_smoke/spark/compute_smoke_risk.py

echo "Risk computation complete."
