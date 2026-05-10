#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

SKIP_BOOTSTRAP="${SKIP_BOOTSTRAP:-1}"
RUN_BOOTSTRAP="${RUN_BOOTSTRAP:-0}"

export FIXTURE_TIME_MODE="${FIXTURE_TIME_MODE:-relative}"
export USE_ALIGNED_FIXTURES="${USE_ALIGNED_FIXTURES:-1}"
export FIRMS_DRY_RUN="${FIRMS_DRY_RUN:-1}"
export OPENAQ_DRY_RUN="${OPENAQ_DRY_RUN:-1}"
export WIND_DRY_RUN="${WIND_DRY_RUN:-1}"
export GRID_WEATHER_DRY_RUN="${GRID_WEATHER_DRY_RUN:-1}"
export GRID_WEATHER_ENABLED="${GRID_WEATHER_ENABLED:-1}"

export PLUME_MODEL_VERSION="${PLUME_MODEL_VERSION:-wind_grid_v2}"
export PLUME_GRID_FALLBACK_TO_STATION="${PLUME_GRID_FALLBACK_TO_STATION:-1}"
export DISPERSION_ENABLED="${DISPERSION_ENABLED:-1}"
export RISK_MODEL_VERSION="${RISK_MODEL_VERSION:-v5}"
export SMOKE_RISK_MODEL_VERSION="${SMOKE_RISK_MODEL_VERSION:-v5}"

export STRICT_ALIGNED_ASSERTS="${STRICT_ALIGNED_ASSERTS:-1}"
export EXPECT_DISPERSION="${EXPECT_DISPERSION:-1}"

echo "==> integration-regression (no live API keys; SKIP_BOOTSTRAP=${SKIP_BOOTSTRAP} RUN_BOOTSTRAP=${RUN_BOOTSTRAP})"

if [[ "${RUN_BOOTSTRAP}" == "1" ]]; then
  echo "==> RUN_BOOTSTRAP=1: make db-bootstrap (may download census)"
  make db-bootstrap
elif [[ "${SKIP_BOOTSTRAP}" != "1" ]]; then
  echo "==> SKIP_BOOTSTRAP!=1 but RUN_BOOTSTRAP!=1 — no bootstrap (set RUN_BOOTSTRAP=1 to force)."
fi

make topics

export REPLAY_RUN_PLUME=0
export REPLAY_RUN_COMPUTE=0

echo "==> Replay FIRMS/OpenAQ/wind fixtures (normalization-only tail; plume/risk run later)"
bash "${ROOT_DIR}/scripts/replay_fixtures.sh"

echo "==> Full normalize (FIRMS + OpenAQ + wind)"
make normalize

echo "==> Wind-only normalize idempotent"
make normalize-wind

echo "==> Grid weather fixture replay"
bash "${ROOT_DIR}/scripts/replay_grid_weather_fixtures.sh"

echo "==> Normalize grid weather"
make normalize-grid-weather

echo "==> Match fires to grid weather"
make match-fire-weather

echo "==> Plume wind_grid_v2"
make compute-plume PLUME_MODEL_VERSION="${PLUME_MODEL_VERSION}"

echo "==> Dispersion gaussian_v0 (DISPERSION_ENABLED=${DISPERSION_ENABLED})"
make compute-dispersion DISPERSION_ENABLED="${DISPERSION_ENABLED}"

echo "==> Risk ${RISK_MODEL_VERSION}"
make compute-risk RISK_MODEL_VERSION="${RISK_MODEL_VERSION}"

echo "==> Dispersion vs AQ lag comparison"
make compare-dispersion-aq DISPERSION_ENABLED="${DISPERSION_ENABLED}"

if [[ "${LOAD_RISK_OBSERVATION_FIXTURES:-0}" == "1" ]]; then
  echo "==> Risk observation fixtures + evaluate-risk (LOAD_RISK_OBSERVATION_FIXTURES=1)"
  make load-risk-observation-fixtures
  RISK_EVAL_MODEL_VERSION="${RISK_MODEL_VERSION}" make evaluate-risk
fi

make collect-lag

make quality-check

ALERTS_WARN_ONLY=1 make alerts-check

ALERTS_DRY_RUN=1 make alerts-materialize

ALERT_NOTIFIER=console make alerts-send

bash "${ROOT_DIR}/scripts/assert_integration_state.sh"

echo "==> Summary"
docker compose exec -T postgres psql -U "${POSTGRES_USER:-smoke}" -d "${POSTGRES_DB:-smoke}" -c "SELECT * FROM analytics.v_integration_pipeline_counts;" || {
  echo "WARN: v_integration_pipeline_counts missing — apply integration/calibration SQL views." >&2
}

echo "integration-regression complete."
