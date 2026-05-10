#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

bash -n "${ROOT_DIR}/scripts/integration_regression.sh"
bash -n "${ROOT_DIR}/scripts/assert_integration_state.sh"
bash -n "${ROOT_DIR}/scripts/evaluate_risk_model.sh"
bash -n "${ROOT_DIR}/scripts/run_compute_dispersion.sh"
bash -n "${ROOT_DIR}/scripts/run_compare_dispersion_aq.sh"
bash -n "${ROOT_DIR}/scripts/dispersion_demo.sh"
bash -n "${ROOT_DIR}/scripts/load_risk_observation_fixtures.sh"
bash -n "${ROOT_DIR}/scripts/calibration_summary.sh"
bash -n "${ROOT_DIR}/scripts/calibration_demo.sh"
bash "${ROOT_DIR}/scripts/evaluate_risk_model.sh"

echo "integration-smoke-test OK"
