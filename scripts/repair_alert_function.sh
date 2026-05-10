#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"
COMPOSE="${COMPOSE:-docker compose}"

echo "==> repair_alert_function: refresh dependent views (Phase 10–12)"
for view in \
  zzz_phase10_10_integration_and_calibration_views.sql \
  zzz_phase11_dispersion_views.sql \
  zzz_phase12_calibration_views.sql; do
  echo "  -> ${view}"
  ${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f - \
    <"${ROOT_DIR}/sql/views/${view}"
done

echo "==> repair_alert_function: apply canonical migration 013 (drop overloads + fn + v_alert_candidates)"
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f - \
  <"${ROOT_DIR}/sql/migrations/013_phase14_canonical_alert_function.sql"

echo "repair_alert_function OK"
