#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export DRY_RUN="${DRY_RUN:-1}"

py() {
  if command -v uv >/dev/null 2>&1; then
    uv sync --extra dev >/dev/null
    uv run python -m wildfire_smoke.compact_parse_errors "$@"
  else
    PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}" python3 -m wildfire_smoke.compact_parse_errors "$@"
  fi
}

echo "==> Parse error lifecycle (DRY_RUN=${DRY_RUN}; set DRY_RUN=0 --no-dry-run to archive aged rows)"
if [[ "${DRY_RUN}" == "0" ]]; then
  py --no-dry-run "$@"
else
  py "$@"
fi
