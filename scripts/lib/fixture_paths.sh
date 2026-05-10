#!/usr/bin/env bash
# shellcheck shell=bash
# Optional aligned fixture paths for integration demos (Phase 10).

apply_aligned_fixture_paths() {
  if [[ "${USE_ALIGNED_FIXTURES:-0}" != "1" ]]; then
    return 0
  fi
  export FIRMS_FIXTURE_CSV="${FIRMS_FIXTURE_CSV:-tests/fixtures/firms_aligned_sample.csv}"
  export OPENAQ_FIXTURE_JSONL="${OPENAQ_FIXTURE_JSONL:-tests/fixtures/openaq_aligned_sample.jsonl}"
  export WIND_FIXTURE_JSONL="${WIND_FIXTURE_JSONL:-tests/fixtures/wind_aligned_sample.jsonl}"
  export GRID_WEATHER_FIXTURE_JSON="${GRID_WEATHER_FIXTURE_JSON:-tests/fixtures/nws_gridpoint_aligned_sample.json}"
}
