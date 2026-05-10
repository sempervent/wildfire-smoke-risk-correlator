#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"
POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

echo "==> calibration-summary (Phase 12 views)"

${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<'SQL'
SELECT 'v_dispersion_aq_evidence_summary' AS view_name, COUNT(*)::bigint AS rows FROM analytics.v_dispersion_aq_evidence_summary;
SELECT 'v_dispersion_aq_lag_summary' AS view_name, COUNT(*)::bigint AS rows FROM analytics.v_dispersion_aq_lag_summary;
SELECT 'v_risk_model_evaluation_latest' AS view_name, COUNT(*)::bigint AS rows FROM analytics.v_risk_model_evaluation_latest;
SELECT 'v_calibration_confidence_summary' AS view_name, COUNT(*)::bigint AS rows FROM analytics.v_calibration_confidence_summary;
SELECT 'v_risk_observation_coverage' AS view_name, COUNT(*)::bigint AS rows FROM analytics.v_risk_observation_coverage;
SQL

echo "==> sample rows"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
  "SELECT * FROM analytics.v_dispersion_aq_evidence_summary ORDER BY comparison_row_count DESC LIMIT 25;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
  "SELECT * FROM analytics.v_risk_model_evaluation_latest LIMIT 10;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
  "SELECT * FROM analytics.v_calibration_confidence_summary;"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
  "SELECT * FROM analytics.v_risk_observation_coverage ORDER BY observation_rows DESC LIMIT 25;"

echo "calibration-summary OK."
