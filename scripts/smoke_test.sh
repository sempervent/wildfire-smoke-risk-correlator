#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ "${SMOKE_NO_COMPOSE:-0}" == "1" ]]; then
  echo "==> SMOKE_NO_COMPOSE=1: fast host checks (no Docker Compose / Postgres required)"

  echo "==> bash syntax (scripts/*.sh)"
  shopt -s nullglob
  for f in "${ROOT_DIR}/scripts/"*.sh; do
    bash -n "$f"
  done
  shopt -u nullglob

  echo "==> Grafana dashboard JSON"
  python3 -m json.tool "${ROOT_DIR}/docker/grafana/dashboards/smoke-risk.json" >/dev/null

  echo "==> Phase 13/14 release / CI artifacts present"
  test -f "${ROOT_DIR}/docs/release/v1.0.0.md"
  test -f "${ROOT_DIR}/docs/release/v1.0.0-checklist.md"
  test -f "${ROOT_DIR}/CHANGELOG.md"
  test -f "${ROOT_DIR}/docs/architecture/system-overview.md"
  test -f "${ROOT_DIR}/docs/architecture/dataflow.md"
  test -f "${ROOT_DIR}/docs/architecture/operational-model.md"
  test -f "${ROOT_DIR}/.github/workflows/ci.yml"
  test -f "${ROOT_DIR}/.github/workflows/integration.yml"

  echo "==> calibration export dry-run (no DB writes to analytics views)"
  if command -v uv >/dev/null 2>&1; then
    uv sync --extra dev >/dev/null
    CALIBRATION_EXPORT_DRY_RUN=1 uv run python -m wildfire_smoke.export_calibration
    echo "==> db_doctor module import"
    uv run python -c "import wildfire_smoke.db_doctor"
    echo "==> release manifest dry-run"
    RELEASE_MANIFEST_DRY_RUN=1 uv run python -m wildfire_smoke.release_manifest --dry-run >/dev/null
    echo "==> make version"
    make -C "${ROOT_DIR}" version
  else
    echo "WARN: uv not installed; skipping export dry-run + version smoke." >&2
  fi

  echo "Smoke test (no-compose) passed."
  exit 0
fi

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
  firms.hotspots.dlq \
  openaq.measurements.raw \
  openaq.measurements.dlq \
  weather.wind.raw \
  weather.wind.dlq \
  weather.wind.normalized \
  normalization.errors \
  fire.detections.normalized \
  air_quality.measurements.normalized \
  smoke.risk.scores \
  deadletter.events; do
  if ! ${COMPOSE} exec -T redpanda rpk topic describe "${t}" --brokers 127.0.0.1:9092 >/dev/null 2>&1; then
    echo "ERROR: missing topic ${t} (run make topics)." >&2
    exit 1
  fi
done

echo "==> Producer dry-run mode (fixtures; no live NASA/OpenAQ/NWS calls)"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"
uv sync --extra dev >/dev/null
FIRMS_DRY_RUN=1 OPENAQ_DRY_RUN=1 WIND_DRY_RUN=1 uv run python -m wildfire_smoke.producers.firms_producer
FIRMS_DRY_RUN=1 OPENAQ_DRY_RUN=1 WIND_DRY_RUN=1 uv run python -m wildfire_smoke.producers.openaq_producer
FIRMS_DRY_RUN=1 OPENAQ_DRY_RUN=1 WIND_DRY_RUN=1 uv run python -m wildfire_smoke.producers.wind_producer

echo "==> Phase 7 durable parse_errors / offset evidence tables exist"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.parse_errors;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.kafka_consumer_offsets;"

echo "==> Malformed fixture publisher + DLQ replay dry-run (no API keys)"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"
bash "${ROOT_DIR}/scripts/replay_bad_fixtures.sh"
DRY_RUN=1 bash "${ROOT_DIR}/scripts/replay_dlq.sh"

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

echo "==> Phase 7 DLQ / parse-error views compile / are queryable"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_parse_errors_open;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_parse_error_summary;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_parse_errors_recent;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_consumer_offset_state;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_dlq_operational_summary;"

echo "==> Phase 8 broker lag / replay bookkeeping tables"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.kafka_topic_offsets;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.kafka_consumer_lag_observations;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.dlq_replay_runs;"

echo "==> Phase 8 lag / pipeline views compile"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_kafka_topic_depth;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_consumer_lag_latest;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_dlq_topic_depth;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_pipeline_lag_summary;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_dlq_replay_runs;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_dlq_replay_items_recent;"

echo "==> collect-lag (best-effort; STRICT_LAG_COLLECTION not set)"
bash "${ROOT_DIR}/scripts/collect_kafka_lag.sh" || true

echo "==> replay-dlq bookkeeping dry-run creates replay run row when DB available"
RUNS_BEFORE="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*)::bigint FROM analytics.dlq_replay_runs;" | tr -d '[:space:]')"
DRY_RUN=1 DLQ_REPLAY_BOOKKEEPING=1 bash "${ROOT_DIR}/scripts/replay_dlq.sh" --limit 2 || true
RUNS_AFTER="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*)::bigint FROM analytics.dlq_replay_runs;" | tr -d '[:space:]')"
if (( RUNS_AFTER <= RUNS_BEFORE )); then
  echo "WARN: expected dlq_replay_runs to grow after replay-dlq (bookkeeping enabled)." >&2
fi

echo "==> parse-errors-compact dry-run"
DRY_RUN=1 bash "${ROOT_DIR}/scripts/compact_parse_errors.sh" || true

test -f "${ROOT_DIR}/docs/runbooks/kafka-lag-high.md"
test -f "${ROOT_DIR}/docs/runbooks/dlq-depth-high.md"
test -f "${ROOT_DIR}/docs/runbooks/replay-failures-recent.md"
test -f "${ROOT_DIR}/docs/runbooks/grid-weather-stale.md"
test -f "${ROOT_DIR}/docs/runbooks/no-recent-grid-weather.md"
test -f "${ROOT_DIR}/docs/runbooks/fire-weather-unmatched-high.md"
test -f "${ROOT_DIR}/docs/runbooks/integration-pipeline-incomplete.md"
test -f "${ROOT_DIR}/docs/runbooks/v4-risk-missing.md"
test -f "${ROOT_DIR}/docs/runbooks/fire-weather-match-missing.md"
test -f "${ROOT_DIR}/docs/runbooks/high-dispersion-exposure.md"
test -f "${ROOT_DIR}/docs/runbooks/dispersion-no-wind-matches.md"
test -f "${ROOT_DIR}/docs/runbooks/dispersion-aq-mismatch-high.md"
test -f "${ROOT_DIR}/docs/runbooks/dispersion-no-targets.md"

echo "==> Phase 9 gridded weather (tables / views / topics)"
for t in weather.grid.raw weather.grid.dlq weather.grid.normalized; do
  ${COMPOSE} exec -T redpanda rpk topic describe "${t}" --brokers 127.0.0.1:9092 >/dev/null
done
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM normalized.weather_grid_cells;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_latest_weather_grid_cells;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_grid_weather_operational_summary;"

if [[ "${GRID_WEATHER_SMOKE:-0}" == "1" ]]; then
  echo "==> GRID_WEATHER_SMOKE=1: grid-weather demo chain (Spark + plume v2 + risk v4)"
  bash "${ROOT_DIR}/scripts/grid_weather_demo.sh"
fi

echo "==> Phase 10 integration scripts / calibration hooks"
bash -n "${ROOT_DIR}/scripts/integration_regression.sh"
bash -n "${ROOT_DIR}/scripts/assert_integration_state.sh"
bash -n "${ROOT_DIR}/scripts/evaluate_risk_model.sh"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.risk_observations;" >/dev/null 2>&1 || echo "WARN: analytics.risk_observations missing (apply migration 010)." >&2
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.v_integration_pipeline_counts;" >/dev/null 2>&1 || echo "WARN: analytics.v_integration_pipeline_counts missing (apply Phase 10 views)." >&2
bash "${ROOT_DIR}/scripts/evaluate_risk_model.sh"

echo "==> Phase 11 dispersion scripts / DDL hooks"
bash -n "${ROOT_DIR}/scripts/run_compute_dispersion.sh"
bash -n "${ROOT_DIR}/scripts/run_compare_dispersion_aq.sh"
bash -n "${ROOT_DIR}/scripts/dispersion_demo.sh"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.smoke_dispersion_exposures;" >/dev/null 2>&1 || echo "WARN: analytics.smoke_dispersion_exposures missing (apply migration 011)." >&2

if [[ "${DISPERSION_SMOKE:-0}" == "1" ]]; then
  echo "==> DISPERSION_SMOKE=1: dispersion demo chain (Spark-heavy)"
  bash "${ROOT_DIR}/scripts/dispersion_demo.sh"
fi

echo "==> Phase 12 calibration scripts / SQL hooks"
bash -n "${ROOT_DIR}/scripts/load_risk_observation_fixtures.sh"
bash -n "${ROOT_DIR}/scripts/calibration_summary.sh"
bash -n "${ROOT_DIR}/scripts/calibration_demo.sh"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM analytics.risk_observation_features;" >/dev/null 2>&1 || echo "WARN: analytics.risk_observation_features missing (apply migration 012)." >&2
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT 1 FROM analytics.v_dispersion_aq_evidence_summary LIMIT 1;" >/dev/null 2>&1 || echo "WARN: Phase 12 calibration views missing (apply zzz_phase12_calibration_views.sql)." >&2

if [[ "${CALIBRATION_SMOKE:-0}" == "1" ]]; then
  echo "==> CALIBRATION_SMOKE=1: calibration demo chain (Spark-heavy)"
  bash "${ROOT_DIR}/scripts/calibration_demo.sh"
fi

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

if [[ "${DLQ_SMOKE:-0}" == "1" ]]; then
  echo "==> DLQ_SMOKE=1: full DLQ smoke (replay bad fixtures + normalize + parse_errors assertions)"
  bash "${ROOT_DIR}/scripts/dlq_smoke_test.sh"
fi

echo "Smoke test passed."
