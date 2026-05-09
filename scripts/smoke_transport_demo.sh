#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export WIND_DRY_RUN="${WIND_DRY_RUN:-1}"
export FIRMS_DRY_RUN="${FIRMS_DRY_RUN:-1}"
export OPENAQ_DRY_RUN="${OPENAQ_DRY_RUN:-1}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

echo "==> Smoke transport demo: fixtures + normalize + plume + risk v3 snapshot"

bash "${ROOT_DIR}/scripts/replay_fixtures.sh"

echo "==> Optional risk v3 row (same window as v2; requires plume rows for non-zero plume blend)"
SMOKE_RISK_MODEL_VERSION=v3 bash "${ROOT_DIR}/scripts/run_compute_risk.sh"

echo "Smoke transport demo finished."
