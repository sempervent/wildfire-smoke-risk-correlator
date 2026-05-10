#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PROJECT="${RELEASE_COMPOSE_PROJECT_NAME:-wildfire-smoke-release-test}"
if [[ -z "${PROJECT// }" ]]; then
  echo "ERROR: RELEASE_COMPOSE_PROJECT_NAME resolved empty." >&2
  exit 2
fi

echo "================================================================"
echo "release_fresh_volume_test: ISOLATED Compose project=${PROJECT}"
echo "This script must NOT target your normal dev stack."
echo "================================================================"

export COMPOSE_PROJECT_NAME="${PROJECT}"
export COMPOSE="docker compose --project-name ${COMPOSE_PROJECT_NAME}"
export MINIMAL_CENSUS_REPLACE_ALL="${MINIMAL_CENSUS_REPLACE_ALL:-1}"

cleanup() {
  if [[ "${KEEP_RELEASE_STACK:-0}" == "1" ]]; then
    echo "KEEP_RELEASE_STACK=1: leaving project ${COMPOSE_PROJECT_NAME} running."
    return 0
  fi
  echo "==> tearing down ${COMPOSE_PROJECT_NAME} (volumes removed)"
  ${COMPOSE} down -v --remove-orphans || true
}

trap cleanup EXIT

echo "==> stopping any prior isolated project stack"
${COMPOSE} down -v --remove-orphans || true

echo "==> starting postgres + redpanda + spark (release test slice)"
${COMPOSE} up -d postgres redpanda redpanda-console spark-master spark-worker

echo "==> waiting for Postgres"
for _ in $(seq 1 60); do
  if ${COMPOSE} exec -T postgres pg_isready -U "${POSTGRES_USER:-smoke}" -d "${POSTGRES_DB:-smoke}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if [[ "${FULL_CENSUS:-0}" == "1" ]]; then
  echo "==> FULL_CENSUS=1: db-bootstrap (downloads Census — slow)"
  make db-bootstrap
else
  echo "==> minimal census bootstrap + migrations/views"
  make db-bootstrap-minimal
fi

echo "==> topics"
make topics

echo "==> integration-smoke-test"
make integration-smoke-test

echo "==> db-doctor"
make db-doctor

if [[ "${CALIBRATION_EXPORT_DRY_RUN:-1}" == "1" ]]; then
  echo "==> export-calibration dry-run"
  CALIBRATION_EXPORT_DRY_RUN=1 uv run python -m wildfire_smoke.export_calibration || true
else
  echo "==> export-calibration (real)"
  make export-calibration || echo "WARN: export-calibration failed (often missing optional data)." >&2
fi

echo "==> version"
make version

echo "================================================================"
echo "release_fresh_volume_test: SUCCESS"
echo "================================================================"

if [[ "${KEEP_RELEASE_STACK:-0}" == "1" ]]; then
  trap - EXIT
fi
