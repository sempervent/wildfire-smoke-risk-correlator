#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"
POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

STRICT_ALIGNED_ASSERTS="${STRICT_ALIGNED_ASSERTS:-0}"
EXPECT_PARSE_ERRORS="${EXPECT_PARSE_ERRORS:-0}"
EXPECT_DISPERSION="${EXPECT_DISPERSION:-0}"

fail() {
  echo "ASSERT_FAILED: $*" >&2
  exit 1
}

warn() {
  echo "ASSERT_WARN: $*" >&2
}

psql_exec() {
  ${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" "$@"
}

echo "==> assert-integration-state (STRICT_ALIGNED_ASSERTS=${STRICT_ALIGNED_ASSERTS})"

if ! ${COMPOSE} exec -T postgres pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
  fail "PostgreSQL not reachable"
fi

have_relation() {
  local sch="$1"
  local rel="$2"
  local cnt
  cnt="$(psql_exec -At -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${sch}' AND table_name='${rel}';")"
  [[ "${cnt}" == "1" ]]
}

have_relation normalized fire_detections || fail "missing normalized.fire_detections"
have_relation normalized air_quality_measurements || fail "missing normalized.air_quality_measurements"
have_relation normalized wind_observations || fail "missing normalized.wind_observations"
have_relation normalized weather_grid_cells || fail "missing normalized.weather_grid_cells"
have_relation analytics fire_weather_matches || fail "missing analytics.fire_weather_matches"
have_relation analytics smoke_plume_exposures || fail "missing analytics.smoke_plume_exposures"
have_relation analytics smoke_risk_scores || fail "missing analytics.smoke_risk_scores"
have_relation analytics kafka_topic_offsets || fail "missing analytics.kafka_topic_offsets"
have_relation analytics kafka_consumer_lag_observations || fail "missing analytics.kafka_consumer_lag_observations"
have_relation analytics parse_errors || fail "missing analytics.parse_errors"
have_relation analytics notification_attempts || fail "missing analytics.notification_attempts"

if [[ "${EXPECT_DISPERSION}" == "1" ]]; then
  have_relation analytics smoke_dispersion_exposures || fail "missing analytics.smoke_dispersion_exposures"
  have_relation analytics dispersion_aq_comparisons || fail "missing analytics.dispersion_aq_comparisons"
fi

FIRE_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM normalized.fire_detections;")"
AQ_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM normalized.air_quality_measurements;")"
WIND_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM normalized.wind_observations;")"
GRID_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM normalized.weather_grid_cells;")"
MATCH_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.fire_weather_matches;")"
PLUME_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.smoke_plume_exposures WHERE model_version='wind_grid_v2';")"
V4_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.smoke_risk_scores WHERE model_version='v4';")"
V5_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.smoke_risk_scores WHERE model_version='v5';")"
DISP_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.smoke_dispersion_exposures;")"
OFF_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.kafka_topic_offsets;")"
LAG_N="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.kafka_consumer_lag_observations;")"

[[ "${FIRE_N}" =~ ^[0-9]+$ ]] || fail "bad fire count"
[[ "${AQ_N}" =~ ^[0-9]+$ ]] || fail "bad aq count"

if [[ "${FIRE_N}" -lt 1 ]]; then
  fail "expected normalized.fire_detections count >= 1, got ${FIRE_N}"
fi
if [[ "${AQ_N}" -lt 1 ]]; then
  fail "expected normalized.air_quality_measurements count >= 1, got ${AQ_N}"
fi
if [[ "${WIND_N}" -lt 1 ]]; then
  fail "expected normalized.wind_observations count >= 1, got ${WIND_N}"
fi
if [[ "${GRID_N}" -lt 1 ]]; then
  fail "expected normalized.weather_grid_cells count >= 1, got ${GRID_N}"
fi

if [[ "${OFF_N}" -lt 1 ]]; then
  fail "expected analytics.kafka_topic_offsets count >= 1 after collect-lag, got ${OFF_N}"
fi
if [[ "${LAG_N}" -lt 1 ]]; then
  warn "analytics.kafka_consumer_lag_observations empty (broker lag tooling may not have sampled yet)"
fi

if [[ "${V4_N}" -lt 1 ]]; then
  warn "no analytics.smoke_risk_scores rows for model_version=v4 (expected when RISK_MODEL_VERSION=v4)"
fi
if [[ "${V5_N}" -lt 1 ]]; then
  warn "no analytics.smoke_risk_scores rows for model_version=v5 (expected when RISK_MODEL_VERSION=v5)"
fi

if [[ "${STRICT_ALIGNED_ASSERTS}" == "1" ]]; then
  if [[ "${MATCH_N}" -lt 1 ]]; then
    fail "STRICT_ALIGNED_ASSERTS: expected analytics.fire_weather_matches >= 1, got ${MATCH_N}"
  fi
  if [[ "${PLUME_N}" -lt 1 ]]; then
    fail "STRICT_ALIGNED_ASSERTS: expected wind_grid_v2 plume rows >= 1, got ${PLUME_N}"
  fi
  if [[ "${EXPECT_DISPERSION}" == "1" ]]; then
    if [[ "${DISP_N}" -lt 1 ]]; then
      fail "EXPECT_DISPERSION=1: expected analytics.smoke_dispersion_exposures >= 1, got ${DISP_N}"
    fi
    if [[ "${V5_N}" -lt 1 ]]; then
      fail "EXPECT_DISPERSION=1: expected v5 risk rows >= 1, got ${V5_N}"
    fi
  else
    if [[ "${V4_N}" -lt 1 ]]; then
      fail "STRICT_ALIGNED_ASSERTS: expected v4 risk rows >= 1, got ${V4_N}"
    fi
  fi
fi

OPEN_PE="$(psql_exec -At -c "SELECT COUNT(*)::text FROM analytics.parse_errors WHERE status='open';")"
if [[ "${EXPECT_PARSE_ERRORS}" == "0" ]] && [[ "${OPEN_PE}" != "0" ]]; then
  warn "open parse_errors=${OPEN_PE} (EXPECT_PARSE_ERRORS=0)"
fi

echo "assert-integration-state OK (fires=${FIRE_N} aq=${AQ_N} wind=${WIND_N} grid=${GRID_N} matches=${MATCH_N} plume_v2=${PLUME_N} v4=${V4_N} v5=${V5_N} dispersion=${DISP_N} offsets=${OFF_N} lag_obs=${LAG_N})"
