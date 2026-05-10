#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"
POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

psql_exec() {
  ${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" "$@"
}

echo "==> DLQ smoke: publish malformed fixtures"
bash "${ROOT_DIR}/scripts/replay_bad_fixtures.sh"

echo "==> DLQ smoke: run Spark normalizers (must not crash on bad rows)"
bash "${ROOT_DIR}/scripts/run_normalize.sh"

echo "==> DLQ smoke: expect analytics.parse_errors rows from malformed payloads"
CNT="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.parse_errors;")"
if [[ "${CNT}" == "0" ]]; then
  echo "ERROR: parse_errors is empty after replay-bad-fixtures + normalize (expected quarantine rows)." >&2
  exit 2
fi

echo "parse_errors_row_count=${CNT}"

echo "==> DLQ smoke: replay_dlq dry-run (postgres source)"
DRY_RUN=1 bash "${ROOT_DIR}/scripts/replay_dlq.sh"

echo "==> DLQ smoke: Phase 7 views compile"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_parse_errors_open;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_parse_error_summary;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_parse_errors_recent;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_consumer_offset_state;"
psql_exec -c "SELECT COUNT(*) FROM analytics.v_dlq_operational_summary;"

echo "DLQ smoke test passed."
