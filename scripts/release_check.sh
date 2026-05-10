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

echo "==> release_check: fast smoke (no Compose)"
SMOKE_NO_COMPOSE=1 bash "${ROOT_DIR}/scripts/smoke_test.sh"

echo "==> release_check: make version"
make version

echo "==> release_check: release docs + changelog"
test -f "${ROOT_DIR}/docs/release/v1.0.0.md"
test -f "${ROOT_DIR}/docs/release/v1.0.1.md"
test -f "${ROOT_DIR}/CHANGELOG.md"
grep -qiE '(v1\.0\.0|\[1\.0\.0\])' "${ROOT_DIR}/CHANGELOG.md"
grep -qiE '(v1\.0\.1|\[1\.0\.1\])' "${ROOT_DIR}/CHANGELOG.md"

echo "==> release_check: AGENTS.md Phase 14 release / drift invariants"
grep -qi "Phase 14" "${ROOT_DIR}/AGENTS.md"
grep -qi "fn_alert_candidates" "${ROOT_DIR}/AGENTS.md"

echo "==> release_check: .env.example Phase 14 knobs"
for sym in \
  CALIBRATION_MIN_AQ_OBSERVATIONS \
  LOAD_RISK_OBSERVATION_FIXTURES \
  RISK_EVAL_MIN_MATCH_COUNT \
  CALIBRATION_EXPORT_DIR \
  CALIBRATION_EXPORT_FORMATS \
  COMPOSE_INTEGRATION \
  MINIMAL_CENSUS_REPLACE_ALL \
  DB_DOCTOR_WARN_ONLY \
  FULL_CENSUS \
  KEEP_RELEASE_STACK \
  FULL_RELEASE_TEST \
  RELEASE_MANIFEST_DRY_RUN \
  RELEASE_COMPOSE_PROJECT_NAME; do
  grep -q "${sym}" "${ROOT_DIR}/.env.example" || {
    echo "ERROR: .env.example missing documented symbol ${sym}" >&2
    exit 1
  }
done

echo "==> release_check: optional-dependencies parquet in pyproject.toml"
grep -q 'parquet' "${ROOT_DIR}/pyproject.toml"

echo "==> release_check: db_doctor / repair / fresh-volume scripts import/syntax"
uv run python -c "import wildfire_smoke.db_doctor; import wildfire_smoke.release_manifest"
bash -n "${ROOT_DIR}/scripts/db_doctor.sh"
bash -n "${ROOT_DIR}/scripts/repair_alert_function.sh"
bash -n "${ROOT_DIR}/scripts/release_fresh_volume_test.sh"
bash -n "${ROOT_DIR}/scripts/write_release_manifest.sh"

echo "==> release_check: CI workflow YAML files"
test -f "${ROOT_DIR}/.github/workflows/ci.yml"
test -f "${ROOT_DIR}/.github/workflows/integration.yml"

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

if [[ "${COMPOSE_INTEGRATION:-0}" == "1" ]]; then
  echo "==> release_check: Compose integration gates (COMPOSE_INTEGRATION=1)"
  make integration-smoke-test
  make db-doctor
  CALIBRATION_EXPORT_DRY_RUN=1 uv run python -m wildfire_smoke.export_calibration || true
else
  echo "==> release_check: skipping Compose gates (set COMPOSE_INTEGRATION=1 for db-doctor / export dry-run)"
fi

if [[ "${FULL_RELEASE_TEST:-0}" == "1" ]]; then
  echo "==> release_check: isolated fresh-volume test (FULL_RELEASE_TEST=1)"
  make release-fresh-volume-test
else
  echo "==> release_check: skipping release-fresh-volume-test (set FULL_RELEASE_TEST=1)"
fi

echo "release_check OK"
