#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"
POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
GRAFANA_PORT="${GRAFANA_PORT:-3001}"

DEMO_REFRESH_MVIEWS="${DEMO_REFRESH_MVIEWS:-1}"

echo "==> Demo: bring stack up"
make up

echo "==> Demo: census + SQL migrations/views"
make db-bootstrap

echo "==> Demo: Kafka topics"
make topics

echo "==> Demo: fixture replay (no API keys)"
make replay-fixtures

echo "==> Demo: normalize + plume + risk (replay-fixtures may have run these; safe to repeat)"
make normalize
make compute-plume
make compute-risk

echo "==> Demo: quality check"
make quality-check

if [[ "${DEMO_REFRESH_MVIEWS}" == "1" ]]; then
  echo "==> Demo: refresh materialized views"
  make refresh-mviews
fi

echo ""
echo "=== Local demo URLs ==="
echo "Grafana:        http://localhost:${GRAFANA_PORT}"
echo "Redpanda UI:    http://localhost:8088"
echo "Spark UI:       http://localhost:8091"
echo ""
echo "=== Postgres (host) ==="
echo "psql postgresql://${POSTGRES_USER}:***@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
echo ""
echo "=== Useful psql (inside Compose) ==="
echo "${COMPOSE} exec -T postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c \"SELECT * FROM analytics.v_alert_candidates LIMIT 20;\""
echo "${COMPOSE} exec -T postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c \"SELECT * FROM analytics.v_latest_smoke_risk_county_geojson LIMIT 5;\""
echo "${COMPOSE} exec -T postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c \"SELECT statefp, COUNT(*) FROM geo.tracts GROUP BY statefp ORDER BY 1;\""
echo ""
echo "Demo finished."
