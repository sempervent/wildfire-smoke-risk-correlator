-- Phase 6: wind + plume presentation surfaces for Grafana / operators.

CREATE OR REPLACE VIEW analytics.v_latest_wind_observations AS
SELECT *
FROM (
  SELECT *
  FROM normalized.wind_observations
  ORDER BY observed_at DESC NULLS LAST
  LIMIT 500
) x;

CREATE OR REPLACE VIEW analytics.v_latest_wind_observations_geojson AS
SELECT
  w.wind_observation_id,
  w.source,
  w.station_id,
  w.observed_at,
  w.latitude,
  w.longitude,
  w.wind_speed_mps,
  w.wind_direction_degrees,
  w.wind_gust_mps,
  w.county_geoid,
  w.tract_geoid,
  ST_AsGeoJSON(w.geom)::jsonb AS geojson
FROM analytics.v_latest_wind_observations w;

CREATE OR REPLACE VIEW analytics.v_latest_smoke_plume_exposures AS
SELECT *
FROM (
  SELECT *
  FROM analytics.smoke_plume_exposures
  ORDER BY computed_at DESC NULLS LAST
  LIMIT 500
) x;

CREATE OR REPLACE VIEW analytics.v_top_plume_exposures AS
SELECT *
FROM (
  SELECT
    p.*,
    ROW_NUMBER() OVER (
      ORDER BY p.exposure_score DESC NULLS LAST, p.computed_at DESC NULLS LAST
    ) AS exposure_rank
  FROM analytics.smoke_plume_exposures p
  WHERE p.computed_at >= (now() - interval '7 days')
) s
WHERE s.exposure_rank <= 100;

CREATE OR REPLACE VIEW analytics.v_latest_smoke_risk_v3 AS
SELECT *
FROM analytics.v_latest_smoke_risk_by_county
WHERE model_version = 'v3'
UNION ALL
SELECT *
FROM analytics.v_latest_smoke_risk_by_tract
WHERE model_version = 'v3';

CREATE OR REPLACE VIEW analytics.v_smoke_transport_summary AS
SELECT
  (SELECT COUNT(*)::bigint FROM normalized.wind_observations) AS wind_observations_total,
  (
    SELECT COUNT(*)::bigint
    FROM normalized.wind_observations w
    WHERE w.observed_at >= (now() - interval '24 hours')
  ) AS wind_observations_last_24h,
  (SELECT MAX(observed_at) FROM normalized.wind_observations) AS newest_wind_observed_at,
  (SELECT COUNT(*)::bigint FROM analytics.smoke_plume_exposures) AS plume_exposure_rows_total,
  (
    SELECT COUNT(*)::bigint
    FROM analytics.smoke_plume_exposures p
    WHERE p.computed_at >= (now() - interval '24 hours')
  ) AS plume_exposure_rows_last_24h,
  (SELECT MAX(exposure_score) FROM analytics.smoke_plume_exposures) AS max_plume_exposure_score_overall,
  (SELECT MAX(computed_at) FROM analytics.smoke_plume_exposures) AS newest_plume_computed_at;

COMMENT ON VIEW analytics.v_latest_wind_observations IS
  'Recent normalized wind rows (presentation). Meteorological wind direction is wind FROM.';
COMMENT ON VIEW analytics.v_smoke_transport_summary IS
  'High-level counters for wind ingestion + plume corridor snapshots (engineering metrics only).';
