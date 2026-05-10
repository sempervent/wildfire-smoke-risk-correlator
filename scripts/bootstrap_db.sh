#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

COMPOSE="${COMPOSE:-docker compose}"

PHASE14_MIGRATION_BASENAME="013_phase14_canonical_alert_function.sql"
PHASE14_MIGRATION="${ROOT_DIR}/sql/migrations/${PHASE14_MIGRATION_BASENAME}"

echo "Applying SQL migrations from sql/migrations (except ${PHASE14_MIGRATION_BASENAME})..."
shopt -s nullglob
for f in "${ROOT_DIR}/sql/migrations/"*.sql; do
  [[ -f "${f}" ]] || continue
  [[ "$(basename "${f}")" == "${PHASE14_MIGRATION_BASENAME}" ]] && continue
  echo "  -> $(basename "${f}")"
  ${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f - <"${f}"
done
shopt -u nullglob

echo "Applying SQL views from sql/views..."
for f in "${ROOT_DIR}/sql/views/"*.sql; do
  [[ -f "${f}" ]] || continue
  echo "  -> $(basename "${f}")"
  ${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f - <"${f}"
done

if [[ -f "${PHASE14_MIGRATION}" ]]; then
  echo "Applying ${PHASE14_MIGRATION_BASENAME} last (canonical alert function + v_alert_candidates)..."
  ${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f - <"${PHASE14_MIGRATION}"
fi

echo "Bootstrap DB views applied."
