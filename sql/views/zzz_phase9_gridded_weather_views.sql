-- Phase 9: gridded weather, fire–weather matching, plume v2 / risk v4 presentation views.

CREATE OR REPLACE VIEW analytics.v_latest_weather_grid_cells AS
SELECT DISTINCT ON (weather_cell_id)
  weather_cell_id,
  source,
  grid_id,
  valid_time,
  forecast_time,
  latitude,
  longitude,
  wind_speed_mps,
  wind_direction_degrees,
  temperature_c,
  relative_humidity_percent,
  geom,
  county_geoid,
  tract_geoid,
  inserted_at
FROM normalized.weather_grid_cells
ORDER BY weather_cell_id, valid_time DESC NULLS LAST, inserted_at DESC;

COMMENT ON VIEW analytics.v_latest_weather_grid_cells IS 'Latest snapshot per weather_cell_id for dashboards.';

CREATE OR REPLACE VIEW analytics.v_latest_weather_grid_cells_geojson AS
SELECT
  weather_cell_id,
  source,
  grid_id,
  valid_time,
  latitude,
  longitude,
  wind_speed_mps,
  wind_direction_degrees,
  temperature_c,
  relative_humidity_percent,
  json_build_object(
    'type',
    'Feature',
    'geometry',
    ST_AsGeoJSON(geom)::json,
    'properties',
    json_build_object(
      'weather_cell_id',
      weather_cell_id,
      'wind_speed_mps',
      wind_speed_mps,
      'wind_direction_degrees',
      wind_direction_degrees,
      'temperature_c',
      temperature_c,
      'relative_humidity_percent',
      relative_humidity_percent
    )
  )::text AS geojson
FROM analytics.v_latest_weather_grid_cells;

COMMENT ON VIEW analytics.v_latest_weather_grid_cells_geojson IS 'Point GeoJSON for latest grid cells (presentation-only).';

CREATE OR REPLACE VIEW analytics.v_fire_weather_matches AS
SELECT
  m.fire_weather_match_id,
  m.detection_id,
  m.weather_cell_id,
  m.match_method,
  m.distance_km,
  m.time_delta_minutes,
  m.wind_speed_mps,
  m.wind_direction_degrees,
  m.temperature_c,
  m.relative_humidity_percent,
  m.matched_at,
  c.latitude AS cell_latitude,
  c.longitude AS cell_longitude,
  c.valid_time AS cell_valid_time
FROM analytics.fire_weather_matches m
LEFT JOIN normalized.weather_grid_cells c ON c.weather_cell_id = m.weather_cell_id;

COMMENT ON VIEW analytics.v_fire_weather_matches IS 'Fire ↔ grid-cell matches with cell metadata.';

CREATE OR REPLACE VIEW analytics.v_fire_weather_match_summary AS
SELECT
  match_method,
  COUNT(*)::bigint AS match_rows,
  COUNT(DISTINCT detection_id)::bigint AS distinct_fires,
  AVG(distance_km)::double precision AS avg_distance_km,
  MAX(matched_at) AS newest_match_at
FROM analytics.fire_weather_matches
GROUP BY match_method;

COMMENT ON VIEW analytics.v_fire_weather_match_summary IS 'Aggregate fire–weather match coverage.';

CREATE OR REPLACE VIEW analytics.v_latest_smoke_plume_exposures_v2 AS
SELECT e.*
FROM analytics.smoke_plume_exposures e
WHERE e.model_version = 'wind_grid_v2'
  AND (e.window_start, e.window_end) IN (
    SELECT window_start, window_end
    FROM analytics.smoke_plume_exposures
    WHERE model_version = 'wind_grid_v2'
    ORDER BY computed_at DESC NULLS LAST
    LIMIT 1
  );

COMMENT ON VIEW analytics.v_latest_smoke_plume_exposures_v2 IS 'Latest wind_grid_v2 plume batch (engineering heuristic).';

CREATE OR REPLACE VIEW analytics.v_latest_smoke_risk_v4 AS
SELECT s.*
FROM analytics.smoke_risk_scores s
WHERE s.model_version = 'v4'
  AND (s.window_start, s.window_end) IN (
    SELECT window_start, window_end
    FROM analytics.smoke_risk_scores
    WHERE model_version = 'v4'
    ORDER BY computed_at DESC NULLS LAST
    LIMIT 1
  );

COMMENT ON VIEW analytics.v_latest_smoke_risk_v4 IS 'Latest v4 risk batch for the active scoring window.';

CREATE OR REPLACE VIEW analytics.v_grid_weather_operational_summary AS
SELECT
  (SELECT COUNT(*)::bigint FROM normalized.weather_grid_cells) AS total_cells,
  (
    SELECT COUNT(*)::bigint
    FROM normalized.weather_grid_cells
    WHERE valid_time >= now() - interval '24 hours'
  ) AS cells_24h,
  (SELECT MAX(valid_time) FROM normalized.weather_grid_cells) AS newest_valid_time,
  (SELECT COUNT(*)::bigint FROM analytics.fire_weather_matches) AS total_matches,
  (
    SELECT COUNT(*)::bigint
    FROM analytics.smoke_plume_exposures
    WHERE model_version = 'wind_grid_v2'
  ) AS plume_wind_grid_v2_rows,
  (
    SELECT COUNT(*)::bigint
    FROM analytics.smoke_risk_scores
    WHERE model_version = 'v4'
  ) AS risk_v4_rows,
  now() AS summary_at;

COMMENT ON VIEW analytics.v_grid_weather_operational_summary IS 'High-level grid ingest / match / downstream row counts.';
