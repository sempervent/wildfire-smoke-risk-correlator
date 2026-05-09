#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ -z "${FIRMS_MAP_KEY:-}" ]]; then
  echo "ERROR: FIRMS_MAP_KEY is required for bounded live FIRMS ingestion." >&2
  echo "Use fixture replay (make replay-fixtures) or operational-cycle with LIVE_MODE=0 for no secrets." >&2
  exit 1
fi

export FIRMS_DRY_RUN="${FIRMS_DRY_RUN:-0}"
export OPENAQ_DRY_RUN="${OPENAQ_DRY_RUN:-0}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

DEFAULT_BBOX="-88.2,34.9,-81.6,36.7"
export LIVE_INGEST_BBOX="${LIVE_INGEST_BBOX:-${DEFAULT_BBOX}}"
export FIRMS_BBOX="${FIRMS_BBOX:-${LIVE_INGEST_BBOX}}"
export OPENAQ_BBOX="${OPENAQ_BBOX:-${LIVE_INGEST_BBOX}}"

echo "==> Live ingest configuration (no secrets printed)"
echo "    FIRMS_DRY_RUN=${FIRMS_DRY_RUN} OPENAQ_DRY_RUN=${OPENAQ_DRY_RUN}"
echo "    LIVE_INGEST_BBOX=${LIVE_INGEST_BBOX}"
echo "    FIRMS_BBOX=${FIRMS_BBOX}"
echo "    OPENAQ_BBOX=${OPENAQ_BBOX}"
echo "    LIVE_INGEST_MAX_SPAN_DEG=${LIVE_INGEST_MAX_SPAN_DEG:-<default 14>} LIVE_INGEST_ALLOW_LARGE_BBOX=${LIVE_INGEST_ALLOW_LARGE_BBOX:-0}"

uv sync --extra dev >/dev/null
uv run python -c "from wildfire_smoke.live_bbox import assert_bbox_allowed_for_live_ingest; assert_bbox_allowed_for_live_ingest()"

echo "==> Producers (live)"
bash "${ROOT_DIR}/scripts/ingest_once.sh"

echo "==> Normalize"
bash "${ROOT_DIR}/scripts/run_normalize.sh"

echo "==> Compute risk"
bash "${ROOT_DIR}/scripts/run_compute_risk.sh"

echo "==> Quality check"
bash "${ROOT_DIR}/scripts/quality_check.sh"

echo "==> Materialize alerts"
bash "${ROOT_DIR}/scripts/materialize_alerts.sh"

echo "==> Send alerts"
export ALERT_NOTIFIER="${ALERT_NOTIFIER:-console}"
bash "${ROOT_DIR}/scripts/send_alerts.sh"

echo "Live ingest cycle complete."
