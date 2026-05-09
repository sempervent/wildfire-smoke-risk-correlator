#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"
POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

echo "==> PostgreSQL reachable"
${COMPOSE} exec -T postgres pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"

echo "==> PostGIS extension present"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT PostGIS_Version();"

echo "==> geo.counties has rows"
COUNTIES="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM geo.counties;")"
if [[ "${COUNTIES}" -lt 1 ]]; then
  echo "ERROR: geo.counties is empty (run make db-bootstrap)." >&2
  exit 1
fi

echo "==> geo.tracts has rows"
TRACTS="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM geo.tracts;")"
if [[ "${TRACTS}" -lt 1 ]]; then
  echo "ERROR: geo.tracts is empty (run make db-bootstrap)." >&2
  exit 1
fi

echo "==> Kafka topics exist"
for t in \
  firms.hotspots.raw \
  openaq.measurements.raw \
  weather.wind.raw \
  weather.wind.normalized \
  fire.detections.normalized \
  air_quality.measurements.normalized \
  smoke.risk.scores \
  deadletter.events; do
  if ! ${COMPOSE} exec -T redpanda rpk topic describe "${t}" --brokers 127.0.0.1:9092 >/dev/null 2>&1; then
    echo "ERROR: missing topic ${t} (run make topics)." >&2
    exit 1
  fi
done

echo "==> Producer dry-run mode (fixtures; no live NASA/OpenAQ calls)"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"
uv sync --extra dev >/dev/null
FIRMS_DRY_RUN=1 OPENAQ_DRY_RUN=1 uv run python -m wildfire_smoke.producers.firms_producer
FIRMS_DRY_RUN=1 OPENAQ_DRY_RUN=1 uv run python -m wildfire_smoke.producers.openaq_producer

echo "==> SQL views compile / are queryable"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.smoke_risk_by_county;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.smoke_risk_by_tract;"
echo "==> Phase 2 analytics views compile / are queryable"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_source_freshness;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_data_quality_summary;"

echo "==> Phase 3 GeoJSON / SLI / alert views compile / are queryable"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_latest_smoke_risk_county_geojson;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_latest_smoke_risk_tract_geojson;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_latest_fire_detections_geojson;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_latest_air_quality_geojson;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_alert_candidates;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_sli_source_freshness;"

echo "==> Plume computation job runs (may insert 0 rows without correlated fires + wind)"
bash "${ROOT_DIR}/scripts/run_compute_plume.sh"

echo "==> Risk computation job runs (may insert 0 rows if no correlated window data)"
bash "${ROOT_DIR}/scripts/run_compute_risk.sh"

echo "==> Phase 6 smoke transport views compile / are queryable"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_latest_wind_observations;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_latest_wind_observations_geojson;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_latest_smoke_plume_exposures;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_top_plume_exposures;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_latest_smoke_risk_v3;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_smoke_transport_summary;"

echo "==> Phase 4 alert persistence + routing (table + dry-run materialize + console send)"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
  "SELECT COUNT(*) FROM analytics.alert_events;"
ALERTS_DRY_RUN=1 bash "${ROOT_DIR}/scripts/materialize_alerts.sh"
ALERT_NOTIFIER=console ALERT_SEVERITY_MIN=warning ALERT_LIMIT=10 FORCE_NOTIFY=1 bash "${ROOT_DIR}/scripts/send_alerts.sh"
bash -n "${ROOT_DIR}/scripts/run_operational_cycle.sh"
bash -n "${ROOT_DIR}/scripts/live_ingest_once.sh"
test -f "${ROOT_DIR}/docs/runbooks/alert-overview.md"

echo "==> Phase 5 notification reliability surfaces"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
  "SELECT COUNT(*) FROM analytics.notification_attempts;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
  "SELECT COUNT(*) FROM analytics.v_open_alert_events;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
  "SELECT COUNT(*) FROM analytics.v_notification_attempt_summary;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
  "SELECT COUNT(*) FROM analytics.v_recent_operational_cycles;"
ALERT_NOTIFIER=console ALERT_DIGEST=1 ALERT_SEVERITY_MIN=warning bash "${ROOT_DIR}/scripts/send_alerts.sh" --digest
ALERT_NOTIFIER=console ALERT_RETRY_QUEUE=1 bash "${ROOT_DIR}/scripts/send_alerts.sh" --retry-queue
test -f "${ROOT_DIR}/docker/scheduler/loop.sh"
test -f "${ROOT_DIR}/deploy/systemd/wildfire-smoke-operational.service"
test -f "${ROOT_DIR}/deploy/systemd/wildfire-smoke-operational.timer"

echo "==> alerts-check (warn-only; fixture data is often stale)"
ALERTS_WARN_ONLY=1 bash "${ROOT_DIR}/scripts/check_alerts.sh"

echo "Smoke test passed."
