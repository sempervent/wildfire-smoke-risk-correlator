#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"
POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

WARN_ONLY="${ALERTS_WARN_ONLY:-0}"

THRESHOLDS_JSON="$(uv run python -c "
from wildfire_smoke.alert_thresholds import alert_thresholds_from_env
import json
t = alert_thresholds_from_env()
print(json.dumps({
  'warn_h': t.freshness_warn_hours,
  'crit_h': t.freshness_critical_hours,
  'risk_min': t.high_risk_min_score,
  'lookback_h': t.lookback_hours,
  'plume_min': t.high_plume_exposure_min_score,
  'parse_warn': t.parse_errors_warn_count,
  'parse_crit': t.parse_errors_critical_count,
  'offset_stale_h': t.consumer_offset_stale_hours,
  'parser_spike_warn': t.parser_spike_warn_count,
  'parser_spike_crit': t.parser_spike_critical_count,
  'kafka_lag_warn': t.kafka_lag_warn_messages,
  'kafka_lag_crit': t.kafka_lag_critical_messages,
  'dlq_depth_warn': t.dlq_depth_warn_messages,
  'dlq_depth_crit': t.dlq_depth_critical_messages,
  'grid_stale_h': t.grid_weather_stale_hours,
  'fw_unmatched_warn': t.fire_weather_unmatched_warn_count,
  'fw_unmatched_crit': t.fire_weather_unmatched_critical_count,
  'disp_high_min': t.high_dispersion_exposure_min_score,
  'disp_no_wind_h': t.dispersion_no_wind_matches_hours,
  'disp_aq_mismatch_min': t.dispersion_aq_mismatch_min_score,
  'model_mismatch_min': t.model_mismatch_min_count,
  'aq_cov_min': t.aq_observation_coverage_min_count,
  'cal_warn_only': 1 if t.calibration_warn_only else 0,
}))
")"

WARN_H="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['warn_h']))")"
CRIT_H="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['crit_h']))")"
RISK_MIN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(float(json.load(sys.stdin)['risk_min']))")"
LB_H="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['lookback_h']))")"
PLUME_MIN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(float(json.load(sys.stdin)['plume_min']))")"
PARSE_WARN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['parse_warn']))")"
PARSE_CRIT="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['parse_crit']))")"
OFFSET_STALE_H="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['offset_stale_h']))")"
SPIKE_WARN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['parser_spike_warn']))")"
SPIKE_CRIT="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['parser_spike_crit']))")"
LAG_WARN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['kafka_lag_warn']))")"
LAG_CRIT="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['kafka_lag_crit']))")"
DLQ_WARN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['dlq_depth_warn']))")"
DLQ_CRIT="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['dlq_depth_crit']))")"
GRID_STALE_H="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['grid_stale_h']))")"
FW_UN_W="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['fw_unmatched_warn']))")"
FW_UN_C="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['fw_unmatched_crit']))")"
DISP_HIGH_MIN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(float(json.load(sys.stdin)['disp_high_min']))")"
DISP_NO_WIND_H="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['disp_no_wind_h']))")"
DISP_AQ_MIN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(float(json.load(sys.stdin)['disp_aq_mismatch_min']))")"
MODEL_MISMATCH_MIN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['model_mismatch_min']))")"
AQ_COV_MIN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['aq_cov_min']))")"
CAL_WARN_ONLY="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['cal_warn_only']))")"

echo "==> Alert thresholds: warn_h=${WARN_H} crit_h=${CRIT_H} risk_min=${RISK_MIN} lookback_h=${LB_H} plume_min=${PLUME_MIN} parse_warn=${PARSE_WARN} parse_crit=${PARSE_CRIT} offset_stale_h=${OFFSET_STALE_H} spike_warn=${SPIKE_WARN} spike_crit=${SPIKE_CRIT} lag_warn=${LAG_WARN} lag_crit=${LAG_CRIT} dlq_warn=${DLQ_WARN} dlq_crit=${DLQ_CRIT} grid_stale_h=${GRID_STALE_H} fw_unmatched_warn=${FW_UN_W} fw_unmatched_crit=${FW_UN_C} disp_high_min=${DISP_HIGH_MIN} disp_no_wind_h=${DISP_NO_WIND_H} disp_aq_mismatch_min=${DISP_AQ_MIN} model_mismatch_min=${MODEL_MISMATCH_MIN} aq_cov_min=${AQ_COV_MIN} cal_warn_only=${CAL_WARN_ONLY} ALERTS_WARN_ONLY=${WARN_ONLY}"

FN_ARGS="${WARN_H}, ${CRIT_H}, ${RISK_MIN}::double precision, ${LB_H}, ${PLUME_MIN}::double precision, ${PARSE_WARN}, ${PARSE_CRIT}, ${OFFSET_STALE_H}, ${SPIKE_WARN}, ${SPIKE_CRIT}, ${LAG_WARN}::bigint, ${LAG_CRIT}::bigint, ${DLQ_WARN}::bigint, ${DLQ_CRIT}::bigint, ${GRID_STALE_H}, ${FW_UN_W}, ${FW_UN_C}, ${DISP_HIGH_MIN}::double precision, ${DISP_NO_WIND_H}, ${DISP_AQ_MIN}::double precision, ${MODEL_MISMATCH_MIN}, ${AQ_COV_MIN}, ${CAL_WARN_ONLY}"

echo "==> Alert candidates"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
SELECT alert_type, severity, geography_type, geoid, title, observed_at
FROM analytics.fn_alert_candidates(${FN_ARGS})
ORDER BY
  CASE severity WHEN 'critical' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END,
  observed_at DESC NULLS LAST;
"

CRIT_COUNT="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
SELECT COUNT(*)::text
FROM analytics.fn_alert_candidates(${FN_ARGS})
WHERE severity = 'critical';
")"

echo "critical_candidate_count=${CRIT_COUNT}"

if [[ "${WARN_ONLY}" == "1" ]]; then
  echo "ALERTS_WARN_ONLY=1: exiting 0 regardless of critical count."
  exit 0
fi

if [[ "${CRIT_COUNT}" != "0" ]]; then
  echo "ERROR: ${CRIT_COUNT} critical alert candidate(s) detected." >&2
  exit 2
fi

echo "No critical alert candidates."
