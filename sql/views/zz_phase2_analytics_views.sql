CREATE OR REPLACE VIEW analytics.v_latest_smoke_risk_by_county AS
WITH ranked AS (
  SELECT
    s.id,
    s.geography_type,
    s.geoid,
    s.window_start,
    s.window_end,
    s.model_version,
    s.fire_count,
    s.nearby_fire_count,
    s.nearest_fire_km,
    s.max_frp,
    s.avg_pm25,
    s.avg_pm10,
    s.aq_observation_count,
    s.newest_aq_observed_at,
    s.newest_fire_observed_at,
    s.risk_score,
    s.risk_band,
    s.explanation,
    s.computed_at,
    ROW_NUMBER() OVER (
      PARTITION BY s.geoid, s.model_version
      ORDER BY s.window_end DESC, s.computed_at DESC
    ) AS rn
  FROM analytics.smoke_risk_scores s
  WHERE s.geography_type = 'county'
)
SELECT
  r.id,
  r.geography_type,
  r.geoid,
  c.name AS geography_name,
  c.statefp,
  c.countyfp,
  r.window_start,
  r.window_end,
  r.model_version,
  r.fire_count,
  r.nearby_fire_count,
  r.nearest_fire_km,
  r.max_frp,
  r.avg_pm25,
  r.avg_pm10,
  r.aq_observation_count,
  r.newest_aq_observed_at,
  r.newest_fire_observed_at,
  r.risk_score,
  r.risk_band,
  r.explanation,
  r.computed_at
FROM ranked r
JOIN geo.counties c ON c.geoid = r.geoid
WHERE r.rn = 1;

CREATE OR REPLACE VIEW analytics.v_latest_smoke_risk_by_tract AS
WITH ranked AS (
  SELECT
    s.id,
    s.geography_type,
    s.geoid,
    s.window_start,
    s.window_end,
    s.model_version,
    s.fire_count,
    s.nearby_fire_count,
    s.nearest_fire_km,
    s.max_frp,
    s.avg_pm25,
    s.avg_pm10,
    s.aq_observation_count,
    s.newest_aq_observed_at,
    s.newest_fire_observed_at,
    s.risk_score,
    s.risk_band,
    s.explanation,
    s.computed_at,
    ROW_NUMBER() OVER (
      PARTITION BY s.geoid, s.model_version
      ORDER BY s.window_end DESC, s.computed_at DESC
    ) AS rn
  FROM analytics.smoke_risk_scores s
  WHERE s.geography_type = 'tract'
)
SELECT
  r.id,
  r.geography_type,
  r.geoid,
  t.name AS geography_name,
  t.statefp,
  t.countyfp,
  r.window_start,
  r.window_end,
  r.model_version,
  r.fire_count,
  r.nearby_fire_count,
  r.nearest_fire_km,
  r.max_frp,
  r.avg_pm25,
  r.avg_pm10,
  r.aq_observation_count,
  r.newest_aq_observed_at,
  r.newest_fire_observed_at,
  r.risk_score,
  r.risk_band,
  r.explanation,
  r.computed_at
FROM ranked r
JOIN geo.tracts t ON t.geoid = r.geoid
WHERE r.rn = 1;

CREATE OR REPLACE VIEW analytics.v_top_smoke_risk_areas AS
WITH unioned AS (
  SELECT
    geography_type,
    geoid,
    geography_name,
    statefp,
    countyfp,
    window_end,
    model_version,
    risk_score,
    risk_band,
    computed_at
  FROM analytics.v_latest_smoke_risk_by_county
  UNION ALL
  SELECT
    geography_type,
    geoid,
    geography_name,
    statefp,
    countyfp,
    window_end,
    model_version,
    risk_score,
    risk_band,
    computed_at
  FROM analytics.v_latest_smoke_risk_by_tract
)
SELECT
  u.*,
  ROW_NUMBER() OVER (
    PARTITION BY u.model_version
    ORDER BY u.risk_score DESC NULLS LAST, u.geoid ASC
  ) AS risk_rank
FROM unioned u;

CREATE OR REPLACE VIEW analytics.v_latest_fire_detections AS
SELECT *
FROM (
  SELECT *
  FROM normalized.fire_detections
  ORDER BY acq_datetime DESC NULLS LAST
  LIMIT 500
) x;

CREATE OR REPLACE VIEW analytics.v_latest_air_quality_measurements AS
SELECT *
FROM (
  SELECT *
  FROM normalized.air_quality_measurements
  ORDER BY measured_at DESC NULLS LAST
  LIMIT 500
) x;

CREATE OR REPLACE VIEW analytics.v_ingestion_run_status AS
SELECT *
FROM (
  SELECT *
  FROM analytics.ingestion_runs
  ORDER BY started_at DESC NULLS LAST
  LIMIT 200
) x;

CREATE OR REPLACE VIEW analytics.v_source_freshness AS
SELECT
  'normalized.fire_detections'::text AS dataset,
  MAX(acq_datetime) AS latest_event_time,
  MAX(inserted_at) AS latest_row_time
FROM normalized.fire_detections
UNION ALL
SELECT
  'normalized.air_quality_measurements'::text,
  MAX(measured_at),
  MAX(inserted_at)
FROM normalized.air_quality_measurements
UNION ALL
SELECT
  'analytics.ingestion_runs:firms'::text,
  MAX(started_at) FILTER (WHERE source = 'firms'),
  MAX(finished_at) FILTER (WHERE source = 'firms')
FROM analytics.ingestion_runs
UNION ALL
SELECT
  'analytics.ingestion_runs:openaq'::text,
  MAX(started_at) FILTER (WHERE source = 'openaq'),
  MAX(finished_at) FILTER (WHERE source = 'openaq')
FROM analytics.ingestion_runs;

CREATE OR REPLACE VIEW analytics.v_data_quality_summary AS
SELECT 'fire_detections_missing_county_geoid'::text AS metric, COUNT(*)::bigint AS value
FROM normalized.fire_detections
WHERE county_geoid IS NULL
UNION ALL
SELECT 'fire_detections_missing_tract_geoid', COUNT(*)
FROM normalized.fire_detections
WHERE tract_geoid IS NULL
UNION ALL
SELECT 'air_quality_missing_county_geoid', COUNT(*)
FROM normalized.air_quality_measurements
WHERE county_geoid IS NULL
UNION ALL
SELECT 'air_quality_missing_tract_geoid', COUNT(*)
FROM normalized.air_quality_measurements
WHERE tract_geoid IS NULL
UNION ALL
SELECT 'geo_counties_invalid_geom', COUNT(*)
FROM geo.counties
WHERE NOT ST_IsValid(geom)
UNION ALL
SELECT 'geo_tracts_invalid_geom', COUNT(*)
FROM geo.tracts
WHERE NOT ST_IsValid(geom)
UNION ALL
SELECT 'fire_detections_duplicate_ids', COUNT(*) FROM (
  SELECT detection_id FROM normalized.fire_detections GROUP BY detection_id HAVING COUNT(*) > 1
) d
UNION ALL
SELECT 'air_quality_duplicate_ids', COUNT(*) FROM (
  SELECT measurement_id FROM normalized.air_quality_measurements GROUP BY measurement_id HAVING COUNT(*) > 1
) d;
