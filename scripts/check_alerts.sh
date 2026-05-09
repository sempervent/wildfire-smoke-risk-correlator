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
}))
")"

WARN_H="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['warn_h']))")"
CRIT_H="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['crit_h']))")"
RISK_MIN="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(float(json.load(sys.stdin)['risk_min']))")"
LB_H="$(echo "${THRESHOLDS_JSON}" | uv run python -c "import sys,json; print(int(json.load(sys.stdin)['lookback_h']))")"

echo "==> Alert thresholds: warn_h=${WARN_H} crit_h=${CRIT_H} risk_min=${RISK_MIN} lookback_h=${LB_H} ALERTS_WARN_ONLY=${WARN_ONLY}"

echo "==> Alert candidates"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
SELECT alert_type, severity, geography_type, geoid, title, observed_at
FROM analytics.fn_alert_candidates(${WARN_H}, ${CRIT_H}, ${RISK_MIN}::double precision, ${LB_H})
ORDER BY
  CASE severity WHEN 'critical' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END,
  observed_at DESC NULLS LAST;
"

CRIT_COUNT="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "
SELECT COUNT(*)::text
FROM analytics.fn_alert_candidates(${WARN_H}, ${CRIT_H}, ${RISK_MIN}::double precision, ${LB_H})
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
