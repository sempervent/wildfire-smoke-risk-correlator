#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "==> load_minimal_census_fixtures (CI/test synthetic geometries — not operational Census data)"

if command -v uv >/dev/null 2>&1; then
  uv run python -m wildfire_smoke.load_minimal_census "$@"
else
  export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"
  python3 -m wildfire_smoke.load_minimal_census "$@"
fi
