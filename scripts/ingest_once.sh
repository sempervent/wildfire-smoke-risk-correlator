#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

uv sync --extra dev >/dev/null

echo "Running FIRMS producer..."
uv run python -m wildfire_smoke.producers.firms_producer

echo "Running OpenAQ producer..."
uv run python -m wildfire_smoke.producers.openaq_producer

echo "Ingestion cycle complete."
