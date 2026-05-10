#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"

COMPUTE_ENV=(
  -e PYTHONPATH=/app/src
  -e PSYCOPG_CONNINFO="host=postgres port=5432 dbname=${POSTGRES_DB:-smoke} user=${POSTGRES_USER:-smoke} password=${POSTGRES_PASSWORD:-smoke}"
  -e DISPERSION_ENABLED="${DISPERSION_ENABLED:-0}"
  -e DISPERSION_MODEL_VERSION="${DISPERSION_MODEL_VERSION:-gaussian_v0}"
)

echo "Python: compare dispersion vs AQ lag windows (DISPERSION_ENABLED=${DISPERSION_ENABLED:-0})..."
${COMPOSE} exec -T \
  "${COMPUTE_ENV[@]}" \
  spark-master python3 /app/src/wildfire_smoke/spark/compare_dispersion_aq.py

echo "Dispersion AQ comparison complete."
