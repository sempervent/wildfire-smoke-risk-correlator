-- Phase 10: integration pipeline counts and calibration presentation views.

CREATE OR REPLACE VIEW analytics.v_integration_pipeline_counts AS
SELECT
  (SELECT COUNT(*)::bigint FROM normalized.fire_detections) AS total_fire_detections,
  (SELECT COUNT(*)::bigint FROM normalized.fire_detections WHERE acq_datetime >= now() - interval '24 hours') AS fires_24h,
  (SELECT COUNT(*)::bigint FROM normalized.air_quality_measurements) AS total_aq,
  (SELECT COUNT(*)::bigint FROM normalized.air_quality_measurements WHERE measured_at >= now() - interval '24 hours') AS aq_24h,
  (SELECT COUNT(*)::bigint FROM normalized.wind_observations) AS total_wind,
  (SELECT COUNT(*)::bigint FROM normalized.wind_observations WHERE observed_at >= now() - interval '24 hours') AS wind_24h,
  (SELECT COUNT(*)::bigint FROM normalized.weather_grid_cells) AS total_grid_cells,
  (SELECT COUNT(*)::bigint FROM normalized.weather_grid_cells WHERE valid_time >= now() - interval '24 hours') AS grid_cells_24h,
  (SELECT COUNT(*)::bigint FROM analytics.fire_weather_matches) AS total_fire_weather_matches,
  (SELECT COUNT(*)::bigint FROM analytics.smoke_plume_exposures WHERE model_version = 'wind_grid_v2') AS plume_wind_grid_v2_rows,
  (SELECT COUNT(*)::bigint FROM analytics.smoke_plume_exposures WHERE model_version = 'wind_v1') AS plume_wind_v1_rows,
  (SELECT COUNT(*)::bigint FROM analytics.smoke_risk_scores WHERE model_version = 'v4') AS risk_v4_rows,
  (SELECT COUNT(*)::bigint FROM analytics.kafka_topic_offsets) AS kafka_topic_offset_rows,
  (SELECT COUNT(*)::bigint FROM analytics.kafka_consumer_lag_observations) AS kafka_consumer_lag_obs_rows,
  now() AS summary_at;

COMMENT ON VIEW analytics.v_integration_pipeline_counts IS 'Row-count snapshot for integration regression / quality summaries.';

CREATE OR REPLACE VIEW analytics.v_latest_risk_v4_explanations AS
SELECT
  geography_type,
  geoid,
  risk_score,
  risk_band,
  window_start,
  window_end,
  computed_at,
  explanation
FROM analytics.v_latest_smoke_risk_v4;

COMMENT ON VIEW analytics.v_latest_risk_v4_explanations IS 'Latest v4 risk rows with explanation JSON (alias of v_latest_smoke_risk_v4 projection).';

CREATE OR REPLACE VIEW analytics.v_fire_weather_unmatched AS
SELECT
  f.detection_id,
  f.acq_datetime,
  f.latitude,
  f.longitude,
  f.county_geoid,
  f.tract_geoid
FROM normalized.fire_detections f
WHERE f.acq_datetime >= now() - interval '24 hours'
  AND NOT EXISTS (
    SELECT 1 FROM analytics.fire_weather_matches m WHERE m.detection_id = f.detection_id
  );

COMMENT ON VIEW analytics.v_fire_weather_unmatched IS 'Recent fires without a fire–weather grid match.';

CREATE OR REPLACE VIEW analytics.v_risk_observations AS
SELECT *
FROM analytics.risk_observations;

COMMENT ON VIEW analytics.v_risk_observations IS 'Risk calibration observations (presentation).';

CREATE OR REPLACE VIEW analytics.v_risk_model_evaluations AS
SELECT *
FROM analytics.risk_model_evaluations;

COMMENT ON VIEW analytics.v_risk_model_evaluations IS 'Risk model evaluation batches.';

CREATE OR REPLACE VIEW analytics.v_risk_calibration_summary AS
SELECT
  (SELECT COUNT(*)::bigint FROM analytics.risk_observations) AS observation_rows,
  (SELECT COUNT(*)::bigint FROM analytics.risk_model_evaluations) AS evaluation_rows,
  (SELECT MAX(evaluated_at) FROM analytics.risk_model_evaluations) AS newest_evaluation_at,
  (SELECT MAX(observed_at) FROM analytics.risk_observations) AS newest_observation_at,
  now() AS summary_at;

COMMENT ON VIEW analytics.v_risk_calibration_summary IS 'High-level calibration table counts.';
