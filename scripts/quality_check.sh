#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"
POSTGRES_USER="${POSTGRES_USER:-smoke}"
POSTGRES_DB="${POSTGRES_DB:-smoke}"

WARNINGS=0

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

warn() {
  echo "WARN: $*" >&2
  WARNINGS=$((WARNINGS + 1))
}

psql_exec() {
  ${COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" "$@"
}

echo "==> PostgreSQL reachable"
if ! ${COMPOSE} exec -T postgres pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
  fail "PostgreSQL is not reachable via compose."
fi

echo "==> Required tables exist"
REQUIRED_TABLES=(
  "geo.counties"
  "geo.tracts"
  "normalized.fire_detections"
  "normalized.air_quality_measurements"
  "analytics.smoke_risk_scores"
  "analytics.ingestion_runs"
)

for rel in "${REQUIRED_TABLES[@]}"; do
  cnt="$(psql_exec -At -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = split_part('${rel}', '.', 1) AND table_name = split_part('${rel}', '.', 2);")"
  if [[ "${cnt}" != "1" ]]; then
    fail "Missing required relation: ${rel} (run make db-bootstrap)."
  fi
done

echo "==> Invalid census geometries (must be zero)"
INV_COUNTIES="$(psql_exec -At -c "SELECT COUNT(*) FROM geo.counties WHERE NOT ST_IsValid(geom);")"
INV_TRACTS="$(psql_exec -At -c "SELECT COUNT(*) FROM geo.tracts WHERE NOT ST_IsValid(geom);")"
if [[ "${INV_COUNTIES}" != "0" ]] || [[ "${INV_TRACTS}" != "0" ]]; then
  fail "Invalid census geometries: counties=${INV_COUNTIES} tracts=${INV_TRACTS}"
fi

echo "==> Duplicate logical IDs (must be zero)"
DUP_FIRES="$(psql_exec -At -c "SELECT COUNT(*) FROM (SELECT detection_id FROM normalized.fire_detections GROUP BY detection_id HAVING COUNT(*) > 1) s;")"
DUP_AQ="$(psql_exec -At -c "SELECT COUNT(*) FROM (SELECT measurement_id FROM normalized.air_quality_measurements GROUP BY measurement_id HAVING COUNT(*) > 1) s;")"
if [[ "${DUP_FIRES}" != "0" ]] || [[ "${DUP_AQ}" != "0" ]]; then
  fail "Duplicate IDs detected: fire=${DUP_FIRES} aq=${DUP_AQ}"
fi

echo "==> Soft checks (warnings only)"

FCOUNT="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.fire_detections;")"
ACOUNT="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.air_quality_measurements;")"
if [[ "${FCOUNT}" == "0" ]] || [[ "${ACOUNT}" == "0" ]]; then
  warn "Normalized tables appear empty (fire=${FCOUNT}, aq=${ACOUNT}). Run ingestion/fixtures + normalization."
fi

NOW_EPOCH="$(date +%s)"
STALE_HOURS="${QUALITY_STALE_WARN_HOURS:-72}"
if [[ "${FCOUNT}" != "0" ]]; then
  LATEST_FIRE="$(psql_exec -At -c "SELECT COALESCE(EXTRACT(EPOCH FROM MAX(acq_datetime))::bigint, 0) FROM normalized.fire_detections;")"
  if [[ "${LATEST_FIRE}" != "0" ]]; then
    DELTA=$((NOW_EPOCH - LATEST_FIRE))
    if [[ "${DELTA}" -gt $((STALE_HOURS * 3600)) ]]; then
      warn "Latest fire acq_datetime is older than ${STALE_HOURS}h (seconds_ago=${DELTA})."
    fi
  fi
fi

if [[ "${ACOUNT}" != "0" ]]; then
  LATEST_AQ="$(psql_exec -At -c "SELECT COALESCE(EXTRACT(EPOCH FROM MAX(measured_at))::bigint, 0) FROM normalized.air_quality_measurements;")"
  if [[ "${LATEST_AQ}" != "0" ]]; then
    DELTA=$((NOW_EPOCH - LATEST_AQ))
    if [[ "${DELTA}" -gt $((STALE_HOURS * 3600)) ]]; then
      warn "Latest AQ measured_at is older than ${STALE_HOURS}h (seconds_ago=${DELTA})."
    fi
  fi
fi

NULL_CO_FIRE="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.fire_detections WHERE county_geoid IS NULL;")"
NULL_TR_FIRE="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.fire_detections WHERE tract_geoid IS NULL;")"
NULL_CO_AQ="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.air_quality_measurements WHERE county_geoid IS NULL;")"
NULL_TR_AQ="$(psql_exec -At -c "SELECT COUNT(*) FROM normalized.air_quality_measurements WHERE tract_geoid IS NULL;")"
if [[ "${NULL_CO_FIRE}" != "0" ]] || [[ "${NULL_TR_FIRE}" != "0" ]] || [[ "${NULL_CO_AQ}" != "0" ]] || [[ "${NULL_TR_AQ}" != "0" ]]; then
  warn "Unmatched geographies: fire_missing_county=${NULL_CO_FIRE} fire_missing_tract=${NULL_TR_FIRE} aq_missing_county=${NULL_CO_AQ} aq_missing_tract=${NULL_TR_AQ}"
fi

RISK_LAST="$(psql_exec -At -c "SELECT COALESCE(EXTRACT(EPOCH FROM MAX(computed_at))::bigint, 0) FROM analytics.smoke_risk_scores;")"
if [[ "${RISK_LAST}" != "0" ]]; then
  DELTA=$((NOW_EPOCH - RISK_LAST))
  if [[ "${DELTA}" -gt $((STALE_HOURS * 3600)) ]]; then
    warn "Latest smoke_risk_scores.computed_at is older than ${STALE_HOURS}h (seconds_ago=${DELTA})."
  fi
fi

echo "Quality check passed (warnings=${WARNINGS})."
