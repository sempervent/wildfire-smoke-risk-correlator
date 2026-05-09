#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RAW_DIR="${ROOT_DIR}/data/raw/census"
mkdir -p "${RAW_DIR}"

META_JSON="$(uv run python -c "
from wildfire_smoke.census_config import load_census_yaml, resolved_state_fps, load_national_counties_full_us
import json
y = load_census_yaml()
print(json.dumps({
  'states': resolved_state_fps(y),
  'years_try': y.get('years_try', [2024]),
  'national_full': load_national_counties_full_us(),
}))
")"

STATES_JSON="$(echo "${META_JSON}" | uv run python -c "import sys,json; print(' '.join(json.load(sys.stdin)['states']))")"
read -r -a STATE_ARRAY <<<"${STATES_JSON}"
NATIONAL_FULL="$(echo "${META_JSON}" | uv run python -c "import sys,json; print(1 if json.load(sys.stdin)['national_full'] else 0)")"

curl_get() {
  local out="$1"
  local url="$2"
  curl -fsSL --retry 3 --retry-delay 2 -A "wildfire-smoke-risk-correlator/0.1 (+local-dev)" -o "${out}" "${url}"
}

download_tract_for_state() {
  local year="$1"
  local statefp="$2"
  local tract_url="https://www2.census.gov/geo/tiger/TIGER${year}/TRACT/tl_${year}_${statefp}_tract.zip"
  local tract_zip="${RAW_DIR}/tl_${year}_${statefp}_tract.zip"
  local extract_dir="${RAW_DIR}/tract_extract_${statefp}"

  rm -rf "${extract_dir}"
  mkdir -p "${extract_dir}"
  rm -f "${tract_zip}"
  echo "  - tract (${statefp}): ${tract_url}"
  if ! curl_get "${tract_zip}" "${tract_url}"; then
    return 1
  fi
  unzip -o -q "${tract_zip}" -d "${extract_dir}"
  return 0
}

download_county_national() {
  local year="$1"
  local url="https://www2.census.gov/geo/tiger/TIGER${year}/COUNTY/tl_${year}_us_county.zip"
  local zipf="${RAW_DIR}/tl_${year}_us_county.zip"
  rm -f "${zipf}"
  rm -rf "${RAW_DIR}/county_extract_us"
  mkdir -p "${RAW_DIR}/county_extract_us"
  echo "  - county (national): ${url}"
  curl_get "${zipf}" "${url}"
  unzip -o -q "${zipf}" -d "${RAW_DIR}/county_extract_us"
}

download_county_pair_single_state() {
  local year="$1"
  local statefp="$2"
  local county_state_url="https://www2.census.gov/geo/tiger/TIGER${year}/COUNTY/tl_${year}_${statefp}_county.zip"
  local county_us_url="https://www2.census.gov/geo/tiger/TIGER${year}/COUNTY/tl_${year}_us_county.zip"
  local county_state_zip="${RAW_DIR}/tl_${year}_${statefp}_county.zip"
  local county_us_zip="${RAW_DIR}/tl_${year}_us_county.zip"

  rm -f "${county_state_zip}" "${county_us_zip}"
  rm -rf "${RAW_DIR}/county_extract"
  mkdir -p "${RAW_DIR}/county_extract"

  if curl_get "${county_state_zip}" "${county_state_url}"; then
    echo "state" >"${RAW_DIR}/.county_source"
    echo "  - county (state extract): ${county_state_url}"
    unzip -o -q "${county_state_zip}" -d "${RAW_DIR}/county_extract"
    return 0
  fi

  if curl_get "${county_us_zip}" "${county_us_url}"; then
    echo "us" >"${RAW_DIR}/.county_source"
    echo "  - county (national extract; filter STATEFP=${statefp} during load): ${county_us_url}"
    unzip -o -q "${county_us_zip}" -d "${RAW_DIR}/county_extract"
    return 0
  fi

  return 1
}

try_year() {
  local year="$1"
  echo "Trying TIGER year ${year}"

  local statefp_single="${STATE_ARRAY[0]}"
  local num_states="${#STATE_ARRAY[@]}"

  for s in "${STATE_ARRAY[@]}"; do
    if ! download_tract_for_state "${year}" "${s}"; then
      echo "ERROR: tract download failed for state ${s} year ${year}" >&2
      return 1
    fi
  done

  if [[ "${NATIONAL_FULL}" == "1" ]]; then
    echo "national_full" >"${RAW_DIR}/.county_mode"
    download_county_national "${year}"
    echo "${year}" >"${RAW_DIR}/.resolved_year"
    printf '%s\n' "${STATE_ARRAY[@]}" >"${RAW_DIR}/.states"
    echo "Resolved Census year: ${year}"
    return 0
  fi

  if [[ "${num_states}" -gt 1 ]]; then
    echo "national_filtered" >"${RAW_DIR}/.county_mode"
    download_county_national "${year}"
    echo "${year}" >"${RAW_DIR}/.resolved_year"
    printf '%s\n' "${STATE_ARRAY[@]}" >"${RAW_DIR}/.states"
    echo "Resolved Census year: ${year}"
    return 0
  fi

  # Single-state path (legacy-compatible)
  printf '%s\n' "${STATE_ARRAY[@]}" >"${RAW_DIR}/.states"
  if download_county_pair_single_state "${year}" "${statefp_single}"; then
    if [[ "$(tr -d '[:space:]' <"${RAW_DIR}/.county_source")" == "state" ]]; then
      echo "state_zip" >"${RAW_DIR}/.county_mode"
    else
      echo "us_filtered_single" >"${RAW_DIR}/.county_mode"
    fi
    echo "${year}" >"${RAW_DIR}/.resolved_year"
    echo "Resolved Census year: ${year}"
    return 0
  fi

  return 1
}

YEARS_CSV="$(echo "${META_JSON}" | uv run python -c "import sys,json; print(','.join(str(x) for x in json.load(sys.stdin)['years_try']))")"
IFS=',' read -r -a YEAR_TRY <<<"${YEARS_CSV}"

for y in "${YEAR_TRY[@]}"; do
  if try_year "${y}"; then
    exit 0
  fi
done

echo "ERROR: Could not download census shapefiles for states: ${STATES_JSON}" >&2
exit 1
