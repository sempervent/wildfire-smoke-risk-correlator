#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

LIVE_MODE="${LIVE_MODE:-0}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"
export ALERT_NOTIFIER="${ALERT_NOTIFIER:-console}"

if [[ "${LIVE_MODE}" == "1" ]]; then
  echo "==> Operational cycle: LIVE_MODE=1 (bounded live ingest pipeline)"
  bash "${ROOT_DIR}/scripts/live_ingest_once.sh"
  echo "Operational cycle complete (live path)."
  exit 0
fi

echo "==> Operational cycle: LIVE_MODE=0 (fixture replay producers + batch jobs; no live API keys required)"
export ALERTS_WARN_ONLY="${ALERTS_WARN_ONLY:-1}"
export FIRMS_DRY_RUN="${FIRMS_DRY_RUN:-1}"
export OPENAQ_DRY_RUN="${OPENAQ_DRY_RUN:-1}"

REPLAY_RUN_NORMALIZE=0 REPLAY_RUN_COMPUTE=0 bash "${ROOT_DIR}/scripts/replay_fixtures.sh"

echo "==> Normalize"
bash "${ROOT_DIR}/scripts/run_normalize.sh"

echo "==> Compute risk"
bash "${ROOT_DIR}/scripts/run_compute_risk.sh"

echo "==> Quality check"
bash "${ROOT_DIR}/scripts/quality_check.sh"

echo "==> Materialize alerts"
bash "${ROOT_DIR}/scripts/materialize_alerts.sh"

echo "==> Send alerts"
bash "${ROOT_DIR}/scripts/send_alerts.sh"

echo "Operational cycle complete (fixture path)."
