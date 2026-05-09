#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

COMPOSE="${COMPOSE:-docker compose}"

STATEFP="$(uv run python -c "import yaml, pathlib; p=pathlib.Path('config/census.yaml'); print(yaml.safe_load(p.read_text())['state']['statefp'])")"
MIN_COUNTIES="$(uv run python -c "import yaml, pathlib; p=pathlib.Path('config/census.yaml'); print(yaml.safe_load(p.read_text())['validation']['min_counties'])")"
MIN_TRACTS="$(uv run python -c "import yaml, pathlib; p=pathlib.Path('config/census.yaml'); print(yaml.safe_load(p.read_text())['validation']['min_tracts'])")"

RAW_DIR="${ROOT_DIR}/data/raw/census"
RESOLVED_YEAR_FILE="${RAW_DIR}/.resolved_year"
[[ -f "${RESOLVED_YEAR_FILE}" ]] || {
  echo "ERROR: Missing ${RESOLVED_YEAR_FILE}. Run scripts/download_census_boundaries.sh first." >&2
  exit 1
}
YEAR="$(cat "${RESOLVED_YEAR_FILE}")"

COUNTY_SOURCE_FILE="${RAW_DIR}/.county_source"
[[ -f "${COUNTY_SOURCE_FILE}" ]] || {
  echo "ERROR: Missing ${COUNTY_SOURCE_FILE}. Re-run scripts/download_census_boundaries.sh." >&2
  exit 1
}
COUNTY_SOURCE="$(tr -d '[:space:]' <"${COUNTY_SOURCE_FILE}")"

if [[ "${COUNTY_SOURCE}" == "state" ]]; then
  COUNTY_SHP="$(find "${RAW_DIR}/county_extract" -maxdepth 1 -name "tl_${YEAR}_${STATEFP}_county.shp" | head -n 1)"
  COUNTY_LAYER="tl_${YEAR}_${STATEFP}_county"
  COUNTY_SQL="SELECT GEOID AS geoid, STATEFP AS statefp, COUNTYFP AS countyfp, NAME AS name, ALAND AS aland, AWATER AS awater FROM ${COUNTY_LAYER}"
elif [[ "${COUNTY_SOURCE}" == "us" ]]; then
  COUNTY_SHP="$(find "${RAW_DIR}/county_extract" -maxdepth 1 -name "tl_${YEAR}_us_county.shp" | head -n 1)"
  COUNTY_LAYER="tl_${YEAR}_us_county"
  COUNTY_SQL="SELECT GEOID AS geoid, STATEFP AS statefp, COUNTYFP AS countyfp, NAME AS name, ALAND AS aland, AWATER AS awater FROM ${COUNTY_LAYER} WHERE STATEFP = '${STATEFP}'"
else
  echo "ERROR: Unsupported county source marker: ${COUNTY_SOURCE}" >&2
  exit 1
fi

TRACT_SHP="$(find "${RAW_DIR}/tract_extract" -maxdepth 1 -name "tl_${YEAR}_${STATEFP}_tract.shp" | head -n 1)"

[[ -n "${COUNTY_SHP}" && -f "${COUNTY_SHP}" ]] || {
  echo "ERROR: County shapefile not found for year ${YEAR} state ${STATEFP} (county_source=${COUNTY_SOURCE})" >&2
  exit 1
}
[[ -n "${TRACT_SHP}" && -f "${TRACT_SHP}" ]] || {
  echo "ERROR: Tract shapefile not found for year ${YEAR} state ${STATEFP}" >&2
  exit 1
}

PG_CONN="host=postgres port=5432 dbname=${POSTGRES_DB} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}"

COUNTY_SHP_DOCKER="/data/census/county_extract/$(basename "${COUNTY_SHP}")"
TRACT_SHP_DOCKER="/data/census/tract_extract/$(basename "${TRACT_SHP}")"

echo "Loading counties into staging..."
${COMPOSE} --profile tools run --rm gdal-utils ogr2ogr -progress -overwrite -f PostgreSQL \
  "PG:${PG_CONN} active_schema=geo" \
  -nln counties_staging \
  -nlt PROMOTE_TO_MULTI \
  -t_srs EPSG:4326 \
  -lco GEOMETRY_NAME=geom \
  -lco SCHEMA=geo \
  -sql "${COUNTY_SQL}" \
  "${COUNTY_SHP_DOCKER}"

echo "Loading tracts into staging..."
${COMPOSE} --profile tools run --rm gdal-utils ogr2ogr -progress -overwrite -f PostgreSQL \
  "PG:${PG_CONN} active_schema=geo" \
  -nln tracts_staging \
  -nlt PROMOTE_TO_MULTI \
  -t_srs EPSG:4326 \
  -lco GEOMETRY_NAME=geom \
  -lco SCHEMA=geo \
  -sql "SELECT GEOID AS geoid, STATEFP AS statefp, COUNTYFP AS countyfp, TRACTCE AS tractce, NAME AS name, ALAND AS aland, AWATER AS awater FROM tl_${YEAR}_${STATEFP}_tract" \
  "${TRACT_SHP_DOCKER}"

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

echo "Validating row counts and SRID..."
${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<SQL
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
