#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

STRICT="${STRICT_LAG_COLLECTION:-0}"

py_collect() {
  if command -v uv >/dev/null 2>&1; then
    uv sync --extra dev >/dev/null
    uv run python -m wildfire_smoke.kafka_lag "$@"
  else
    PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}" python3 -m wildfire_smoke.kafka_lag "$@"
  fi
}

echo "==> Collect Kafka / Redpanda lag evidence (STRICT_LAG_COLLECTION=${STRICT})"
set +e
py_collect "$@"
code=$?
set -e

if [[ "${code}" != "0" ]]; then
  echo "WARN: kafka lag collection exited ${code}" >&2
fi
if [[ "${STRICT}" == "1" ]] && [[ "${code}" != "0" ]]; then
  exit "${code}"
fi
exit "${code}"
