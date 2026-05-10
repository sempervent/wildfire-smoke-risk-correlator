#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"
POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

echo "Refreshing analytics materialized views (CONCURRENTLY where indexes exist)..."
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<'SQL'
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_latest_smoke_risk_by_county;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_latest_smoke_risk_by_tract;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_latest_smoke_risk_county_geojson;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_latest_smoke_risk_tract_geojson;
SQL

echo "Materialized views refreshed."
