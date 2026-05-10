#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"
POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

WARNINGS=0

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

warn() {
  echo "WARN: $*" >&2
  WARNINGS=$((WARNINGS + 1))
}

psql_exec() {
  ${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" "$@"
}

echo "==> PostgreSQL reachable"
if ! ${COMPOSE} exec -T postgres pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
  fail "PostgreSQL is not reachable via compose."
fi

echo "==> Required tables exist"
REQUIRED_TABLES=(
  "geo.counties"
  "geo.tracts"
  "normalized.fire_detections"
  "normalized.air_quality_measurements"
  "normalized.wind_observations"
  "analytics.smoke_plume_exposures"
  "analytics.smoke_risk_scores"
  "analytics.ingestion_runs"
  "analytics.parse_errors"
  "analytics.kafka_consumer_offsets"
  "analytics.kafka_topic_offsets"
  "analytics.kafka_consumer_lag_observations"
  "analytics.dlq_replay_runs"
)

for rel in "${REQUIRED_TABLES[@]}"; do
  cnt="$(psql_exec -At -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = split_part('${rel}', '.', 1) AND table_name = split_part('${rel}', '.', 2);")"
  if [[ "${cnt}" != "1" ]]; then
    fail "Missing required relation: ${rel} (run make db-bootstrap)."
  fi
done

echo "==> Invalid census geometries (must be zero)"
INV_COUNTIES="$(psql_exec -At -c "SELECT COUNT(*) FROM geo.counties WHERE NOT ST_IsValid(geom);")"
INV_TRACTS="$(psql_exec -At -c "SELECT COUNT(*) FROM geo.tracts WHERE NOT ST_IsValid(geom);")"
if [[ "${INV_COUNTIES}" != "0" ]] || [[ "${INV_TRACTS}" != "0" ]]; then
  fail "Invalid census geometries: counties=${INV_COUNTIES} tracts=${INV_TRACTS}"
fi

echo "==> Duplicate logical IDs (must be zero)"
DUP_FIRES="$(psql_exec -At -c "SELECT COUNT(*) FROM (SELECT detection_id FROM normalized.fire_detections GROUP BY detection_id HAVING COUNT(*) > 1) s;")"
DUP_AQ="$(psql_exec -At -c "SELECT COUNT(*) FROM (SELECT measurement_id FROM normalized.air_quality_measurements GROUP BY measurement_id HAVING COUNT(*) > 1) s;")"
if [[ "${DUP_FIRES}" != "0" ]] || [[ "${DUP_AQ}" != "0" ]]; then
  fail "Duplicate IDs detected: fire=${DUP_FIRES} aq=${DUP_AQ}"
fi

echo "==> Soft checks (warnings only)"

FCOUNT="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.fire_detections;")"
ACOUNT="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.air_quality_measurements;")"
if [[ "${FCOUNT}" == "0" ]] || [[ "${ACOUNT}" == "0" ]]; then
  warn "Normalized tables appear empty (fire=${FCOUNT}, aq=${ACOUNT}). Run ingestion/fixtures + normalization."
fi

NOW_EPOCH="$(date +%s)"
STALE_HOURS="${QUALITY_STALE_WARN_HOURS:-72}"
if [[ "${FCOUNT}" != "0" ]]; then
  LATEST_FIRE="$(psql_exec -At -c "SELECT COALESCE(EXTRACT(EPOCH FROM MAX(acq_datetime))::bigint, 0) FROM normalized.fire_detections;")"
  if [[ "${LATEST_FIRE}" != "0" ]]; then
    DELTA=$((NOW_EPOCH - LATEST_FIRE))
    if [[ "${DELTA}" -gt $((STALE_HOURS * 3600)) ]]; then
      warn "Latest fire acq_datetime is older than ${STALE_HOURS}h (seconds_ago=${DELTA})."
    fi
  fi
fi

if [[ "${ACOUNT}" != "0" ]]; then
  LATEST_AQ="$(psql_exec -At -c "SELECT COALESCE(EXTRACT(EPOCH FROM MAX(measured_at))::bigint, 0) FROM normalized.air_quality_measurements;")"
  if [[ "${LATEST_AQ}" != "0" ]]; then
    DELTA=$((NOW_EPOCH - LATEST_AQ))
    if [[ "${DELTA}" -gt $((STALE_HOURS * 3600)) ]]; then
      warn "Latest AQ measured_at is older than ${STALE_HOURS}h (seconds_ago=${DELTA})."
    fi
  fi
fi

NULL_CO_FIRE="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.fire_detections WHERE county_geoid IS NULL;")"
NULL_TR_FIRE="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.fire_detections WHERE tract_geoid IS NULL;")"
NULL_CO_AQ="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.air_quality_measurements WHERE county_geoid IS NULL;")"
NULL_TR_AQ="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.air_quality_measurements WHERE tract_geoid IS NULL;")"
if [[ "${NULL_CO_FIRE}" != "0" ]] || [[ "${NULL_TR_FIRE}" != "0" ]] || [[ "${NULL_CO_AQ}" != "0" ]] || [[ "${NULL_TR_AQ}" != "0" ]]; then
  warn "Unmatched geographies: fire_missing_county=${NULL_CO_FIRE} fire_missing_tract=${NULL_TR_FIRE} aq_missing_county=${NULL_CO_AQ} aq_missing_tract=${NULL_TR_AQ}"
fi

RISK_LAST="$(psql_exec -At -c "SELECT COALESCE(EXTRACT(EPOCH FROM MAX(computed_at))::bigint, 0) FROM analytics.smoke_risk_scores;")"
if [[ "${RISK_LAST}" != "0" ]]; then
  DELTA=$((NOW_EPOCH - RISK_LAST))
  if [[ "${DELTA}" -gt $((STALE_HOURS * 3600)) ]]; then
    warn "Latest smoke_risk_scores.computed_at is older than ${STALE_HOURS}h (seconds_ago=${DELTA})."
  fi
fi

echo "==> Phase 7 Kafka topics (DLQ + normalization.errors)"
for t in firms.hotspots.dlq openaq.measurements.dlq weather.wind.dlq normalization.errors; do
  if ! ${COMPOSE} exec -T redpanda rpk topic describe "${t}" --brokers 127.0.0.1:9092 >/dev/null 2>&1; then
    fail "Missing Kafka topic ${t} (run make topics)."
  fi
done

echo "==> Phase 9 Kafka topics (gridded weather)"
for t in weather.grid.raw weather.grid.dlq weather.grid.normalized; do
  if ! ${COMPOSE} exec -T redpanda rpk topic describe "${t}" --brokers 127.0.0.1:9092 >/dev/null 2>&1; then
    fail "Missing Kafka topic ${t} (run make topics)."
  fi
done

echo "==> Phase 9 gridded weather DDL / views compile"
psql_exec -c "SELECT COUNT(*) FROM normalized.weather_grid_cells;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_latest_weather_grid_cells;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_fire_weather_matches;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_grid_weather_operational_summary;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_latest_smoke_plume_exposures_v2;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_latest_smoke_risk_v4;"

if [[ "${GRID_WEATHER_ENABLED:-0}" == "1" ]]; then
  GRIDC="$(psql_exec -At -c "SELECT COUNT(*)::text FROM normalized.weather_grid_cells WHERE valid_time >= now() - interval '48 hours';")"
  if [[ "${GRIDC}" == "0" ]]; then
    warn "GRID_WEATHER_ENABLED=1 but no weather_grid_cells in the last 48h."
  fi
  FW_UN="$(psql_exec -At -c "
    SELECT COUNT(*)::text FROM normalized.fire_detections f
    WHERE f.acq_datetime >= now() - interval '24 hours'
      AND NOT EXISTS (SELECT 1 FROM analytics.fire_weather_matches m WHERE m.detection_id = f.detection_id);
  ")"
  if [[ "${FW_UN}" =~ ^[0-9]+$ ]] && [[ "${FW_UN}" != "0" ]]; then
    warn "Recent fire detections without fire_weather_matches: ${FW_UN}"
  fi
fi

echo "==> Phase 10 integration / calibration surfaces"
RO_TBL="$(psql_exec -At -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='analytics' AND table_name='risk_observations';")"
if [[ "${RO_TBL}" != "1" ]]; then
  warn "analytics.risk_observations missing (apply sql/migrations/010_phase10_calibration.sql)."
else
  psql_exec -c "SELECT COUNT(*) FROM analytics.risk_observations;" >/dev/null
fi
IPC_VIEW="$(psql_exec -At -c "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='analytics' AND table_name='v_integration_pipeline_counts';")"
if [[ "${IPC_VIEW}" == "1" ]]; then
  psql_exec -c "SELECT * FROM analytics.v_integration_pipeline_counts;"
  if [[ "${GRID_WEATHER_ENABLED:-0}" == "1" ]]; then
    V4R="$(psql_exec -At -c "SELECT risk_v4_rows::text FROM analytics.v_integration_pipeline_counts LIMIT 1;")"
    if [[ "${V4R}" == "0" ]]; then
      warn "GRID_WEATHER_ENABLED=1 but risk_v4_rows=0 (run compute-risk RISK_MODEL_VERSION=v4)."
    fi
    UM="$(psql_exec -At -c "
      SELECT COUNT(*)::text FROM normalized.fire_detections f
      WHERE f.acq_datetime >= now() - interval '24 hours'
        AND NOT EXISTS (SELECT 1 FROM analytics.fire_weather_matches m WHERE m.detection_id = f.detection_id);
    ")"
    GC="$(psql_exec -At -c "SELECT grid_cells_24h::text FROM analytics.v_integration_pipeline_counts LIMIT 1;")"
    if [[ "${EXPECT_ALIGNED_MATCHES:-0}" == "1" ]] && [[ "${GC}" != "0" ]] && [[ "${UM}" != "0" ]]; then
      warn "EXPECT_ALIGNED_MATCHES=1 but recent fires remain unmatched (count=${UM})."
    fi
  fi
else
  warn "analytics.v_integration_pipeline_counts missing (apply sql/views/zzz_phase10_10_integration_and_calibration_views.sql)."
fi

echo "==> Parse errors / consumer offset evidence (warnings)"
OPEN_PE="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.parse_errors WHERE status = 'open';")"
if [[ "${OPEN_PE}" != "0" ]]; then
  warn "Open parse_errors rows=${OPEN_PE} (inspect analytics.v_parse_errors_open / DLQ topics)."
fi

PE_24H="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.parse_errors WHERE last_seen_at >= (now() - interval '24 hours');")"
if [[ "${PE_24H}" != "0" ]]; then
  warn "Parse_errors touched in last 24h: count=${PE_24H}"
fi

PE_TOTAL="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.parse_errors;")"
if [[ "${PE_TOTAL}" != "0" ]]; then
  PE_AGE_H="$(psql_exec -At -c "SELECT EXTRACT(epoch FROM (now() - MAX(last_seen_at))) / 3600.0 FROM analytics.parse_errors;")"
  echo "latest_parse_error_age_hours=${PE_AGE_H}"
fi

OFF_EV="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.kafka_consumer_offsets WHERE consumer_group LIKE 'spark-normalize%';")"
if [[ "${OFF_EV}" == "0" ]]; then
  warn "No analytics.kafka_consumer_offsets rows for spark-normalize% yet (expected until normalizers run in this environment)."
fi

echo "==> Phase 8 lag / DLQ depth views compile"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_kafka_topic_depth;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_consumer_lag_latest;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_dlq_topic_depth;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_pipeline_lag_summary;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_dlq_replay_runs;"

LAG_OBS="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.kafka_consumer_lag_observations;")"
TOP_OFF="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.kafka_topic_offsets;")"
if [[ "${STRICT_QUALITY:-0}" == "1" ]] && [[ "${LAG_OBS}" == "0" ]] && [[ "${TOP_OFF}" == "0" ]]; then
  fail "STRICT_QUALITY=1: no broker lag evidence rows (run make collect-lag)."
fi
if [[ "${LAG_OBS}" == "0" ]] && [[ "${TOP_OFF}" == "0" ]]; then
  warn "No kafka lag observations yet (run make collect-lag after brokers are up)."
fi

THRESH="$(cd "${ROOT_DIR}" && uv run python -c "from wildfire_smoke.alert_thresholds import alert_thresholds_from_env as g; t=g(); print(t.dlq_depth_warn_messages, t.kafka_lag_warn_messages)")"
DLQ_W="$(echo "${THRESH}" | awk '{print $1}')"
LAG_W="$(echo "${THRESH}" | awk '{print $2}')"
DLQ_SUM="$(psql_exec -At -c "SELECT COALESCE(SUM(approx_dlq_messages_proxy), 0)::text FROM analytics.v_dlq_topic_depth;")"
LAG_SUM="$(psql_exec -At -c "SELECT COALESCE(SUM(lag), 0)::text FROM analytics.v_consumer_lag_latest WHERE consumer_group LIKE 'spark-normalize%' AND topic IN ('firms.hotspots.raw','openaq.measurements.raw','weather.wind.raw');")"
if [[ "${DLQ_SUM}" =~ ^[0-9]+$ ]] && [[ "${DLQ_SUM}" -gt "${DLQ_W}" ]]; then
  warn "DLQ depth proxy sum (${DLQ_SUM}) exceeds warn threshold (${DLQ_W})."
fi
if [[ "${LAG_SUM}" =~ ^[0-9]+$ ]] && [[ "${LAG_SUM}" -gt "${LAG_W}" ]]; then
  warn "Raw-topic consumer lag sum (${LAG_SUM}) exceeds warn threshold (${LAG_W})."
fi

echo "Quality check passed (warnings=${WARNINGS})."
