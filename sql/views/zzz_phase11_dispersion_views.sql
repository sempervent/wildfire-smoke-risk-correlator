-- Phase 11: dispersion proxy presentation views (engineering / dashboards).

CREATE OR REPLACE VIEW analytics.v_latest_smoke_dispersion_exposures AS
SELECT d.*
FROM analytics.smoke_dispersion_exposures d
JOIN (
  SELECT model_version, MAX(window_end) AS mx
  FROM analytics.smoke_dispersion_exposures
  GROUP BY model_version
) w ON d.model_version = w.model_version AND d.window_end = w.mx;

COMMENT ON VIEW analytics.v_latest_smoke_dispersion_exposures IS
  'Latest dispersion window per model_version (tie-break by MAX(window_end)).';

CREATE OR REPLACE VIEW analytics.v_top_dispersion_exposures AS
SELECT *
FROM analytics.v_latest_smoke_dispersion_exposures
ORDER BY dispersion_score DESC
LIMIT 100;

COMMENT ON VIEW analytics.v_top_dispersion_exposures IS
  'Top 100 dispersion scores from the latest window (per model_version join).';

CREATE OR REPLACE VIEW analytics.v_dispersion_operational_summary AS
SELECT
  (SELECT COUNT(*)::bigint FROM normalized.fire_detections WHERE acq_datetime >= now() - interval '24 hours') AS fires_24h,
  (SELECT COUNT(*)::bigint FROM analytics.smoke_dispersion_exposures WHERE computed_at >= now() - interval '24 hours')
    AS dispersion_rows_24h,
  (SELECT COUNT(DISTINCT detection_id)::bigint FROM analytics.smoke_dispersion_exposures WHERE computed_at >= now() - interval '24 hours')
    AS dispersion_detections_24h,
  (SELECT COUNT(*)::bigint FROM analytics.smoke_dispersion_exposures WHERE dispersion_score > 0 AND computed_at >= now() - interval '24 hours')
    AS dispersion_positive_scores_24h,
  (SELECT COALESCE(MAX(dispersion_score), 0)::double precision FROM analytics.smoke_dispersion_exposures WHERE computed_at >= now() - interval '24 hours')
    AS max_dispersion_score_24h,
  (SELECT COALESCE(MAX(computed_at), to_timestamp(0)) FROM analytics.smoke_dispersion_exposures) AS last_dispersion_computed_at,
  (
    SELECT COUNT(*)::bigint
    FROM normalized.fire_detections f
    WHERE f.acq_datetime >= now() - interval '24 hours'
      AND EXISTS (SELECT 1 FROM analytics.fire_weather_matches m WHERE m.detection_id = f.detection_id)
      AND NOT EXISTS (
        SELECT 1 FROM analytics.smoke_dispersion_exposures e
        WHERE e.detection_id = f.detection_id
          AND e.computed_at >= now() - interval '24 hours'
      )
  ) AS fires_with_match_but_no_recent_dispersion;

COMMENT ON VIEW analytics.v_dispersion_operational_summary IS
  'Compact dispersion coverage counters for Grafana / alerts (not science-grade QA).';

CREATE OR REPLACE VIEW analytics.v_dispersion_aq_comparisons AS
SELECT *
FROM analytics.dispersion_aq_comparisons
ORDER BY computed_at DESC, geography_type, geoid, lag_bucket;

CREATE OR REPLACE VIEW analytics.v_latest_smoke_risk_v5 AS
SELECT s.*
FROM analytics.smoke_risk_scores s
JOIN (
  SELECT geography_type, geoid, MAX(computed_at) AS mx
  FROM analytics.smoke_risk_scores
  WHERE model_version = 'v5'
  GROUP BY geography_type, geoid
) u
  ON s.geography_type = u.geography_type
 AND s.geoid = u.geoid
 AND s.computed_at = u.mx
WHERE s.model_version = 'v5';

COMMENT ON VIEW analytics.v_latest_smoke_risk_v5 IS
  'Latest v5 risk row per geography (MAX(computed_at)); explanations reference dispersion + plume hooks.';

CREATE OR REPLACE VIEW analytics.v_dispersion_model_debug AS
SELECT
  dispersion_exposure_id,
  model_version,
  detection_id,
  geography_type,
  geoid,
  window_start,
  window_end,
  dispersion_score,
  concentration_proxy,
  distance_km,
  downwind_distance_km,
  crosswind_distance_km,
  wind_speed_mps,
  explanation
FROM analytics.smoke_dispersion_exposures
ORDER BY computed_at DESC
LIMIT 500;

COMMENT ON VIEW analytics.v_dispersion_model_debug IS
  'Truncated raw dispersion explanations for operators (may include debug JSON when enabled).';
