#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

LIVE_MODE="${LIVE_MODE:-0}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"
export ALERT_NOTIFIER="${ALERT_NOTIFIER:-console}"

py() {
  if command -v uv >/dev/null 2>&1; then
    (cd "${ROOT_DIR}" && uv run python "$@")
  else
    PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}" python3 "$@"
  fi
}

if [[ "${LIVE_MODE}" == "1" ]]; then
  echo "==> Operational cycle: LIVE_MODE=1 (bounded live ingest pipeline)"
  RUN_ID="$(py -m wildfire_smoke.operational_runs start --mode live)"
  finish_failed() {
    py -m wildfire_smoke.operational_runs finish --run-id "${RUN_ID}" --status failed --error "${1:-live_cycle_failed}" >/dev/null 2>&1 || true
  }
  trap 'finish_failed operational_live_failed' ERR
  bash "${ROOT_DIR}/scripts/live_ingest_once.sh"
  trap - ERR
  py -m wildfire_smoke.operational_runs finish --run-id "${RUN_ID}" --status succeeded
  echo "Operational cycle complete (live path)."
  exit 0
fi

echo "==> Operational cycle: LIVE_MODE=0 (fixture replay producers + batch jobs; no live API keys required)"
export ALERTS_WARN_ONLY="${ALERTS_WARN_ONLY:-1}"
export FIRMS_DRY_RUN="${FIRMS_DRY_RUN:-1}"
export OPENAQ_DRY_RUN="${OPENAQ_DRY_RUN:-1}"

RUN_ID="$(py -m wildfire_smoke.operational_runs start --mode fixture)"
finish_failed() {
  py -m wildfire_smoke.operational_runs finish --run-id "${RUN_ID}" --status failed --error "${1:-fixture_cycle_failed}" >/dev/null 2>&1 || true
}
trap 'finish_failed operational_fixture_failed' ERR

step() {
  py -m wildfire_smoke.operational_runs step --run-id "${RUN_ID}" --name "$1" --status "$2"
}

step replay start
REPLAY_RUN_NORMALIZE=0 REPLAY_RUN_COMPUTE=0 bash "${ROOT_DIR}/scripts/replay_fixtures.sh"
step replay ok

echo "==> Normalize"
step normalize start
bash "${ROOT_DIR}/scripts/run_normalize.sh"
step normalize ok

echo "==> Compute plume exposures"
step compute_plume start
bash "${ROOT_DIR}/scripts/run_compute_plume.sh"
step compute_plume ok

echo "==> Compute risk"
step compute_risk start
bash "${ROOT_DIR}/scripts/run_compute_risk.sh"
step compute_risk ok

echo "==> Quality check"
step quality_check start
bash "${ROOT_DIR}/scripts/quality_check.sh"
step quality_check ok

echo "==> Materialize alerts"
step alerts_materialize start
bash "${ROOT_DIR}/scripts/materialize_alerts.sh"
step alerts_materialize ok

echo "==> Send alerts"
step alerts_send start
bash "${ROOT_DIR}/scripts/send_alerts.sh"
step alerts_send ok

trap - ERR
py -m wildfire_smoke.operational_runs finish --run-id "${RUN_ID}" --status succeeded

echo "Operational cycle complete (fixture path)."
