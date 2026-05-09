#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RAW_DIR="${ROOT_DIR}/data/raw/census"
mkdir -p "${RAW_DIR}"

STATEFP="$(uv run python -c "import yaml, pathlib; p=pathlib.Path('config/census.yaml'); print(yaml.safe_load(p.read_text())['state']['statefp'])")"
YEARS="$(uv run python -c "import yaml, pathlib; p=pathlib.Path('config/census.yaml'); print(','.join(map(str,yaml.safe_load(p.read_text())['years_try'])))")"

IFS=',' read -r -a YEAR_ARRAY <<<"${YEARS}"

curl_get() {
  local out="$1"
  local url="$2"
  curl -fsSL --retry 3 --retry-delay 2 -A "wildfire-smoke-risk-correlator/0.1 (+local-dev)" -o "${out}" "${url}"
}

download_pair() {
  local year="$1"

  local tract_url="https://www2.census.gov/geo/tiger/TIGER${year}/TRACT/tl_${year}_${STATEFP}_tract.zip"
  local county_state_url="https://www2.census.gov/geo/tiger/TIGER${year}/COUNTY/tl_${year}_${STATEFP}_county.zip"
  local county_us_url="https://www2.census.gov/geo/tiger/TIGER${year}/COUNTY/tl_${year}_us_county.zip"

  local tract_zip="${RAW_DIR}/tl_${year}_${STATEFP}_tract.zip"
  local county_state_zip="${RAW_DIR}/tl_${year}_${STATEFP}_county.zip"
  local county_us_zip="${RAW_DIR}/tl_${year}_us_county.zip"

  rm -f "${tract_zip}" "${county_state_zip}" "${county_us_zip}"

  echo "Trying TIGER year ${year}"

  echo "  - tract: ${tract_url}"
  if ! curl_get "${tract_zip}" "${tract_url}"; then
    echo "ERROR: tract download failed for year ${year}: ${tract_url}" >&2
    return 1
  fi

  if curl_get "${county_state_zip}" "${county_state_url}"; then
    echo "state" >"${RAW_DIR}/.county_source"
    echo "  - county (state extract): ${county_state_url}"
  elif curl_get "${county_us_zip}" "${county_us_url}"; then
    echo "us" >"${RAW_DIR}/.county_source"
    echo "  - county (national extract; will filter STATEFP=${STATEFP} during load): ${county_us_url}"
  else
    echo "ERROR: county download failed for year ${year} (tried state + national extracts)." >&2
    return 1
  fi

  rm -rf "${RAW_DIR}/county_extract" "${RAW_DIR}/tract_extract"
  mkdir -p "${RAW_DIR}/county_extract" "${RAW_DIR}/tract_extract"

  if [[ "$(cat "${RAW_DIR}/.county_source")" == "state" ]]; then
    unzip -o -q "${county_state_zip}" -d "${RAW_DIR}/county_extract"
  else
    unzip -o -q "${county_us_zip}" -d "${RAW_DIR}/county_extract"
  fi

  unzip -o -q "${tract_zip}" -d "${RAW_DIR}/tract_extract"

  echo "${year}" >"${RAW_DIR}/.resolved_year"
  echo "Resolved Census year: ${year}"
  return 0
}

for y in "${YEAR_ARRAY[@]}"; do
  if download_pair "${y}"; then
    exit 0
  fi
done

echo "ERROR: Could not download county+tract shapefiles for state ${STATEFP} for years: ${YEARS}" >&2
exit 1
