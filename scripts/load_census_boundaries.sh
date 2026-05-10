#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

COMPOSE="${COMPOSE:-docker compose}"

RAW_DIR="${ROOT_DIR}/data/raw/census"
RESOLVED_YEAR_FILE="${RAW_DIR}/.resolved_year"
STATES_FILE="${RAW_DIR}/.states"
COUNTY_MODE_FILE="${RAW_DIR}/.county_mode"

[[ -f "${RESOLVED_YEAR_FILE}" ]] || {
  echo "ERROR: Missing ${RESOLVED_YEAR_FILE}. Run scripts/download_census_boundaries.sh first." >&2
  exit 1
}
[[ -f "${STATES_FILE}" ]] || {
  echo "ERROR: Missing ${STATES_FILE}. Re-run scripts/download_census_boundaries.sh." >&2
  exit 1
}

YEAR="$(cat "${RESOLVED_YEAR_FILE}")"

STATE_ARRAY=()
while IFS= read -r line || [[ -n "${line}" ]]; do
  line="$(echo "${line}" | tr -d '\r')"
  [[ -z "${line// }" ]] && continue
  STATE_ARRAY+=("${line}")
done < "${STATES_FILE}"

if [[ "${#STATE_ARRAY[@]}" -lt 1 ]]; then
  echo "ERROR: No state FIPS entries in ${STATES_FILE}" >&2
  exit 1
fi

STATE_IN_SQL="$(uv run python -c "
from wildfire_smoke.census_config import load_census_yaml, resolved_state_fps, state_fps_sql_in_clause
y = load_census_yaml()
print(state_fps_sql_in_clause(resolved_state_fps(y)))
")"

MIN_TOTALS="$(uv run python -c "
from wildfire_smoke.census_config import load_census_yaml, resolved_state_fps, validation_thresholds
y = load_census_yaml()
states = resolved_state_fps(y)
t = validation_thresholds(y, len(states))
print(t.min_total_counties, t.min_total_tracts)
")"
read -r MIN_COUNTIES MIN_TRACTS <<<"${MIN_TOTALS}"

COUNTY_MODE=""
if [[ -f "${COUNTY_MODE_FILE}" ]]; then
  COUNTY_MODE="$(tr -d '[:space:]' <"${COUNTY_MODE_FILE}")"
else
  echo "WARN: ${COUNTY_MODE_FILE} missing; inferring legacy single-state county layout." >&2
  COUNTY_SOURCE_FILE="${RAW_DIR}/.county_source"
  if [[ -f "${RAW_DIR}/county_extract/tl_${YEAR}_${STATE_ARRAY[0]}_county.shp" ]]; then
    COUNTY_MODE="state_zip"
  else
    COUNTY_MODE="us_filtered_single"
  fi
fi

PG_CONN="host=postgres port=5432 dbname=${POSTGRES_DB} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}"

echo "Loading counties (mode=${COUNTY_MODE}, year=${YEAR}, states=${STATE_ARRAY[*]})..."

if [[ "${COUNTY_MODE}" == "national_full" ]]; then
  COUNTY_SHP="$(find "${RAW_DIR}/county_extract_us" -maxdepth 1 -name "tl_${YEAR}_us_county.shp" | head -n 1)"
  COUNTY_LAYER="tl_${YEAR}_us_county"
  COUNTY_SQL="SELECT GEOID AS geoid, STATEFP AS statefp, COUNTYFP AS countyfp, NAME AS name, ALAND AS aland, AWATER AS awater FROM ${COUNTY_LAYER}"
elif [[ "${COUNTY_MODE}" == "national_filtered" ]]; then
  COUNTY_SHP="$(find "${RAW_DIR}/county_extract_us" -maxdepth 1 -name "tl_${YEAR}_us_county.shp" | head -n 1)"
  COUNTY_LAYER="tl_${YEAR}_us_county"
  COUNTY_SQL="SELECT GEOID AS geoid, STATEFP AS statefp, COUNTYFP AS countyfp, NAME AS name, ALAND AS aland, AWATER AS awater FROM ${COUNTY_LAYER} WHERE STATEFP IN (${STATE_IN_SQL})"
elif [[ "${COUNTY_MODE}" == "state_zip" ]]; then
  SF="${STATE_ARRAY[0]}"
  COUNTY_SHP="$(find "${RAW_DIR}/county_extract" -maxdepth 1 -name "tl_${YEAR}_${SF}_county.shp" | head -n 1)"
  COUNTY_LAYER="tl_${YEAR}_${SF}_county"
  COUNTY_SQL="SELECT GEOID AS geoid, STATEFP AS statefp, COUNTYFP AS countyfp, NAME AS name, ALAND AS aland, AWATER AS awater FROM ${COUNTY_LAYER}"
else
  COUNTY_SHP="$(find "${RAW_DIR}/county_extract" -maxdepth 1 -name "tl_${YEAR}_us_county.shp" | head -n 1)"
  COUNTY_LAYER="tl_${YEAR}_us_county"
  SF="${STATE_ARRAY[0]}"
  COUNTY_SQL="SELECT GEOID AS geoid, STATEFP AS statefp, COUNTYFP AS countyfp, NAME AS name, ALAND AS aland, AWATER AS awater FROM ${COUNTY_LAYER} WHERE STATEFP = '${SF}'"
fi

[[ -n "${COUNTY_SHP}" && -f "${COUNTY_SHP}" ]] || {
  echo "ERROR: County shapefile not found (mode=${COUNTY_MODE}, year=${YEAR})." >&2
  exit 1
}

COUNTY_SHP_DOCKER="/data/census/$(basename "$(dirname "${COUNTY_SHP}")")/$(basename "${COUNTY_SHP}")"

${COMPOSE} --profile tools run --rm gdal-utils ogr2ogr -progress -overwrite -f PostgreSQL \
  "PG:${PG_CONN} active_schema=geo" \
  -nln counties_staging \
  -nlt PROMOTE_TO_MULTI \
  -t_srs EPSG:4326 \
  -lco GEOMETRY_NAME=geom \
  -lco SCHEMA=geo \
  -sql "${COUNTY_SQL}" \
  "${COUNTY_SHP_DOCKER}"

echo "Loading tracts (append per state)..."
FIRST=1
for SF in "${STATE_ARRAY[@]}"; do
  TRACT_DIR="${RAW_DIR}/tract_extract_${SF}"
  TRACT_SHP="$(find "${TRACT_DIR}" -maxdepth 1 -name "tl_${YEAR}_${SF}_tract.shp" | head -n 1)"
  [[ -n "${TRACT_SHP}" && -f "${TRACT_SHP}" ]] || {
    echo "ERROR: Tract shapefile not found for state ${SF} under ${TRACT_DIR}" >&2
    exit 1
  }
  TRACT_SHP_DOCKER="/data/census/tract_extract_${SF}/$(basename "${TRACT_SHP}")"
  LAYER="tl_${YEAR}_${SF}_tract"
  SQL_Q="SELECT GEOID AS geoid, STATEFP AS statefp, COUNTYFP AS countyfp, TRACTCE AS tractce, NAME AS name, ALAND AS aland, AWATER AS awater FROM ${LAYER}"

  if [[ "${FIRST}" -eq 1 ]]; then
    ${COMPOSE} --profile tools run --rm gdal-utils ogr2ogr -progress -overwrite -f PostgreSQL \
      "PG:${PG_CONN} active_schema=geo" \
      -nln tracts_staging \
      -nlt PROMOTE_TO_MULTI \
      -t_srs EPSG:4326 \
      -lco GEOMETRY_NAME=geom \
      -lco SCHEMA=geo \
      -sql "${SQL_Q}" \
      "${TRACT_SHP_DOCKER}"
    FIRST=0
  else
    ${COMPOSE} --profile tools run --rm gdal-utils ogr2ogr -progress -append -f PostgreSQL \
      "PG:${PG_CONN} active_schema=geo" \
      -nln tracts_staging \
      -nlt PROMOTE_TO_MULTI \
      -t_srs EPSG:4326 \
      -lco GEOMETRY_NAME=geom \
      -lco SCHEMA=geo \
      -sql "${SQL_Q}" \
      "${TRACT_SHP_DOCKER}"
  fi
done

echo "Promoting staging tables into geo.counties / geo.tracts..."
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<SQL
BEGIN;
UPDATE normalized.fire_detections SET tract_geoid = NULL, county_geoid = NULL;
UPDATE normalized.air_quality_measurements SET tract_geoid = NULL, county_geoid = NULL;

DELETE FROM geo.tracts;
DELETE FROM geo.counties;

INSERT INTO geo.counties (geoid, statefp, countyfp, name, aland, awater, geom)
SELECT geoid, statefp, countyfp, name, aland, awater, geom
FROM geo.counties_staging;
DROP TABLE geo.counties_staging;

INSERT INTO geo.tracts (geoid, statefp, countyfp, tractce, name, aland, awater, geom)
SELECT geoid, statefp, countyfp, tractce, name, aland, awater, geom
FROM geo.tracts_staging;
DROP TABLE geo.tracts_staging;
COMMIT;
SQL

echo "Validating row counts / SRID / per-state breakdown..."
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<SQL
SELECT statefp, COUNT(*) AS counties_in_state FROM geo.counties GROUP BY statefp ORDER BY statefp;
SELECT statefp, COUNT(*) AS tracts_in_state FROM geo.tracts GROUP BY statefp ORDER BY statefp;

SELECT COUNT(*) AS counties FROM geo.counties;
SELECT COUNT(*) AS tracts FROM geo.tracts;

SELECT DISTINCT ST_SRID(geom) AS counties_srid FROM geo.counties LIMIT 1;
SELECT DISTINCT ST_SRID(geom) AS tracts_srid FROM geo.tracts LIMIT 1;

SELECT indexname
FROM pg_indexes
WHERE schemaname = 'geo'
  AND tablename IN ('counties', 'tracts')
ORDER BY tablename, indexname;
SQL

FAIL=0
COUNTIES="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM geo.counties;")"
TRACTS="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM geo.tracts;")"

if [[ "${COUNTIES}" -lt "${MIN_COUNTIES}" ]]; then
  echo "ERROR: Expected at least ${MIN_COUNTIES} counties, got ${COUNTIES}" >&2
  FAIL=1
fi
if [[ "${TRACTS}" -lt "${MIN_TRACTS}" ]]; then
  echo "ERROR: Expected at least ${MIN_TRACTS} tracts, got ${TRACTS}" >&2
  FAIL=1
fi

COUNTIES_SRID="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT ST_SRID(geom) FROM geo.counties LIMIT 1;")"
TRACTS_SRID="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT ST_SRID(geom) FROM geo.tracts LIMIT 1;")"

if [[ "${COUNTIES_SRID}" != "4326" || "${TRACTS_SRID}" != "4326" ]]; then
  echo "ERROR: Expected SRID 4326 for geometries (counties=${COUNTIES_SRID}, tracts=${TRACTS_SRID})." >&2
  FAIL=1
fi

IDX="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM pg_indexes WHERE schemaname='geo' AND tablename='counties' AND indexdef ILIKE '%gist%';")"
if [[ "${IDX}" -lt 1 ]]; then
  echo "ERROR: Expected a GiST index on geo.counties.geom" >&2
  FAIL=1
fi
IDXT="$(${COMPOSE} exec -T postgres psql -At -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT COUNT(*) FROM pg_indexes WHERE schemaname='geo' AND tablename='tracts' AND indexdef ILIKE '%gist%';")"
if [[ "${IDXT}" -lt 1 ]]; then
  echo "ERROR: Expected a GiST index on geo.tracts.geom" >&2
  FAIL=1
fi

if [[ "${FAIL}" -ne 0 ]]; then
  exit "${FAIL}"
fi

echo "Census load OK (counties=${COUNTIES}, tracts=${TRACTS}, SRID=4326)."
