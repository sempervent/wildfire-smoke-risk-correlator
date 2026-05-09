#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

COMPOSE="${COMPOSE:-docker compose}"

echo "Applying SQL migrations from sql/migrations..."
shopt -s nullglob
for f in "${ROOT_DIR}/sql/migrations/"*.sql; do
  [[ -f "${f}" ]] || continue
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

echo "Bootstrap DB views applied."
