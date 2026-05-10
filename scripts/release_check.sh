#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "==> release_check: ruff"
uv run ruff check src tests

echo "==> release_check: pytest"
uv run pytest -q

echo "==> release_check: Grafana dashboard JSON"
python3 -m json.tool docker/grafana/dashboards/smoke-risk.json >/dev/null

echo "==> release_check: bash syntax (scripts/*.sh)"
shopt -s nullglob
for f in "${ROOT_DIR}/scripts/"*.sh; do
  bash -n "$f"
done
shopt -u nullglob

echo "==> release_check: release docs + changelog"
test -f "${ROOT_DIR}/docs/release/v0.1.0.md"
test -f "${ROOT_DIR}/CHANGELOG.md"
grep -qiE '(v0\.1\.0|\[0\.1\.0\])' "${ROOT_DIR}/CHANGELOG.md"

echo "==> release_check: .env.example Phase 12/13 knobs"
for sym in \
  CALIBRATION_MIN_AQ_OBSERVATIONS \
  LOAD_RISK_OBSERVATION_FIXTURES \
  RISK_EVAL_MIN_MATCH_COUNT \
  CALIBRATION_EXPORT_DIR \
  CALIBRATION_EXPORT_FORMATS \
  COMPOSE_INTEGRATION \
  MINIMAL_CENSUS_REPLACE_ALL; do
  grep -q "${sym}" "${ROOT_DIR}/.env.example" || {
    echo "ERROR: .env.example missing documented symbol ${sym}" >&2
    exit 1
  }
done

echo "==> release_check: runbook markdown paths"
uv run python -c "
from pathlib import Path
import yaml
root = Path('.')
data = yaml.safe_load(Path('config/runbooks.yaml').read_text(encoding='utf-8'))
for k, v in data['mappings'].items():
    p = root / v['path']
    assert p.is_file(), (k, p)
"

echo "==> release_check: smoke-test (requires Compose stack)"
make smoke-test

if [[ "${COMPOSE_INTEGRATION:-0}" == "1" ]]; then
  echo "==> release_check: integration-smoke-test (COMPOSE_INTEGRATION=1)"
  make integration-smoke-test
else
  echo "==> release_check: skipping integration-smoke-test (set COMPOSE_INTEGRATION=1 to enable)"
fi

echo "release_check OK"
