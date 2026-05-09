#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

uv sync --extra dev >/dev/null
exec uv run python -m wildfire_smoke.alerts materialize "$@"
