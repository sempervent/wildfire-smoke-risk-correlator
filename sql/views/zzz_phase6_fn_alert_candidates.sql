-- Phase 6: extend alert candidates for wind freshness + corridor plume scores.
CREATE OR REPLACE FUNCTION analytics.fn_alert_candidates(
  p_warn_hours integer DEFAULT 6,
  p_crit_hours integer DEFAULT 24,
  p_high_risk_min double precision DEFAULT 75,
  p_lookback_hours integer DEFAULT 24,
  p_high_plume_exposure_min double precision DEFAULT 70
)
RETURNS TABLE (
  alert_type text,
  severity text,
  geography_type text,
  geoid text,
  title text,
  description text,
  observed_at timestamptz,
  details jsonb
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    'ingestion_failed'::text AS alert_type,
    'critical'::text AS severity,
    NULL::text AS geography_type,
    NULL::text AS geoid,
    format('Ingestion failed: %s', ir.source) AS title,
    COALESCE(ir.error_message, 'status=failed') AS description,
    COALESCE(ir.finished_at, ir.started_at) AS observed_at,
    jsonb_build_object(
      'run_id', ir.run_id,
      'source', ir.source,
      'mode', ir.mode,
      'records_fetched', ir.records_fetched,
      'records_published', ir.records_published
    ) AS details
  FROM analytics.ingestion_runs ir
  WHERE ir.status = 'failed'
    AND COALESCE(ir.finished_at, ir.started_at) >= (now() - (p_lookback_hours || ' hours')::interval)

  UNION ALL

  SELECT
    'stale_firms_normalized'::text,
    CASE
      WHEN fin.max_acq IS NULL THEN 'critical'
      WHEN fin.max_acq < (now() - (p_crit_hours || ' hours')::interval) THEN 'critical'
      WHEN fin.max_acq < (now() - (p_warn_hours || ' hours')::interval) THEN 'warn'
      ELSE 'warn'
    END,
    NULL,
    NULL,
    'Stale FIRMS-derived fire timestamps'::text,
    format(
      'MAX(normalized.fire_detections.acq_datetime) = %s',
      fin.max_acq
    ),
    fin.max_acq,
    jsonb_build_object(
      'max_acq_datetime', fin.max_acq,
      'warn_hours', p_warn_hours,
      'critical_hours', p_crit_hours
    )
  FROM (
    SELECT MAX(acq_datetime) AS max_acq
    FROM normalized.fire_detections
  ) fin
  WHERE fin.max_acq IS NULL
     OR fin.max_acq < (now() - (p_warn_hours || ' hours')::interval)

  UNION ALL

  SELECT
    'stale_openaq_normalized'::text,
    CASE
      WHEN aq.max_m IS NULL THEN 'critical'
      WHEN aq.max_m < (now() - (p_crit_hours || ' hours')::interval) THEN 'critical'
      WHEN aq.max_m < (now() - (p_warn_hours || ' hours')::interval) THEN 'warn'
      ELSE 'warn'
    END,
    NULL,
    NULL,
    'Stale OpenAQ-derived measurement timestamps'::text,
    format(
      'MAX(normalized.air_quality_measurements.measured_at) = %s',
      aq.max_m
    ),
    aq.max_m,
    jsonb_build_object(
      'max_measured_at', aq.max_m,
      'warn_hours', p_warn_hours,
      'critical_hours', p_crit_hours
    )
  FROM (
    SELECT MAX(measured_at) AS max_m
    FROM normalized.air_quality_measurements
  ) aq
  WHERE aq.max_m IS NULL
     OR aq.max_m < (now() - (p_warn_hours || ' hours')::interval)

  UNION ALL

  SELECT
    'no_recent_fire_detections'::text,
    CASE
      WHEN cnt.cnt = 0 THEN 'critical'
      WHEN cnt.mx IS NOT NULL AND cnt.mx < (now() - (p_crit_hours || ' hours')::interval) THEN 'critical'
      ELSE 'warn'
    END,
    NULL,
    NULL,
    'No / insufficient recent normalized fire rows'::text,
    format(
      'rows_in_lookback=%s newest_acq_in_window=%s',
      cnt.cnt,
      cnt.mx
    ),
    cnt.mx,
    jsonb_build_object(
      'rows_in_lookback', cnt.cnt,
      'newest_in_window', cnt.mx,
      'lookback_hours', p_lookback_hours
    )
  FROM (
    SELECT
      COUNT(*) FILTER (
        WHERE acq_datetime >= (now() - (p_lookback_hours || ' hours')::interval)
      )::bigint AS cnt,
      MAX(acq_datetime) FILTER (
        WHERE acq_datetime >= (now() - (p_lookback_hours || ' hours')::interval)
      ) AS mx
    FROM normalized.fire_detections
  ) cnt
  WHERE cnt.cnt = 0

  UNION ALL

  SELECT
    'no_recent_air_quality'::text,
    CASE
      WHEN cnt.cnt = 0 THEN 'critical'
      WHEN cnt.mx IS NOT NULL AND cnt.mx < (now() - (p_crit_hours || ' hours')::interval) THEN 'critical'
      ELSE 'warn'
    END,
    NULL,
    NULL,
    'No / insufficient recent normalized AQ rows'::text,
    format(
      'rows_in_lookback=%s newest_measured_in_window=%s',
      cnt.cnt,
      cnt.mx
    ),
    cnt.mx,
    jsonb_build_object(
      'rows_in_lookback', cnt.cnt,
      'newest_in_window', cnt.mx,
      'lookback_hours', p_lookback_hours
    )
  FROM (
    SELECT
      COUNT(*) FILTER (
        WHERE measured_at >= (now() - (p_lookback_hours || ' hours')::interval)
      )::bigint AS cnt,
      MAX(measured_at) FILTER (
        WHERE measured_at >= (now() - (p_lookback_hours || ' hours')::interval)
      ) AS mx
    FROM normalized.air_quality_measurements
  ) cnt
  WHERE cnt.cnt = 0

  UNION ALL

  SELECT
    'high_smoke_risk'::text,
    CASE
      WHEN s.risk_band = 'severe' OR s.risk_score >= p_high_risk_min THEN 'critical'
      ELSE 'warn'
    END,
    s.geography_type::text,
    s.geoid::text,
    format('Elevated smoke risk (%s)', s.risk_band)::text,
    format(
      'geoid=%s score=%s model=%s window_end=%s',
      s.geoid,
      s.risk_score,
      s.model_version,
      s.window_end
    ),
    s.computed_at,
    jsonb_build_object(
      'risk_score', s.risk_score,
      'risk_band', s.risk_band,
      'model_version', s.model_version,
      'window_end', s.window_end,
      'explanation', s.explanation
    )
  FROM analytics.v_latest_smoke_risk_by_county s
  WHERE s.risk_band IN ('high', 'severe')
     OR s.risk_score >= p_high_risk_min

  UNION ALL

  SELECT
    'high_smoke_risk'::text,
    CASE
      WHEN s.risk_band = 'severe' OR s.risk_score >= p_high_risk_min THEN 'critical'
      ELSE 'warn'
    END,
    s.geography_type::text,
    s.geoid::text,
    format('Elevated smoke risk (%s)', s.risk_band)::text,
    format(
      'geoid=%s score=%s model=%s window_end=%s',
      s.geoid,
      s.risk_score,
      s.model_version,
      s.window_end
    ),
    s.computed_at,
    jsonb_build_object(
      'risk_score', s.risk_score,
      'risk_band', s.risk_band,
      'model_version', s.model_version,
      'window_end', s.window_end,
      'explanation', s.explanation
    )
  FROM analytics.v_latest_smoke_risk_by_tract s
  WHERE s.risk_band IN ('high', 'severe')
     OR s.risk_score >= p_high_risk_min

  UNION ALL

  SELECT
    'wind_data_stale'::text,
    CASE
      WHEN w.max_o IS NULL THEN 'critical'
      WHEN w.max_o < (now() - (p_crit_hours || ' hours')::interval) THEN 'critical'
      WHEN w.max_o < (now() - (p_warn_hours || ' hours')::interval) THEN 'warn'
      ELSE 'warn'
    END,
    NULL,
    NULL,
    'Stale normalized wind observation timestamps'::text,
    format(
      'MAX(normalized.wind_observations.observed_at) = %s',
      w.max_o
    ),
    w.max_o,
    jsonb_build_object(
      'max_observed_at', w.max_o,
      'warn_hours', p_warn_hours,
      'critical_hours', p_crit_hours
    )
  FROM (
    SELECT MAX(observed_at) AS max_o
    FROM normalized.wind_observations
  ) w
  WHERE w.max_o IS NULL
     OR w.max_o < (now() - (p_warn_hours || ' hours')::interval)

  UNION ALL

  SELECT
    'no_recent_wind_data'::text,
    CASE
      WHEN cnt.cnt = 0 THEN 'critical'
      WHEN cnt.mx IS NOT NULL AND cnt.mx < (now() - (p_crit_hours || ' hours')::interval) THEN 'critical'
      ELSE 'warn'
    END,
    NULL,
    NULL,
    'No / insufficient recent normalized wind rows'::text,
    format(
      'rows_in_lookback=%s newest_observed_in_window=%s',
      cnt.cnt,
      cnt.mx
    ),
    cnt.mx,
    jsonb_build_object(
      'rows_in_lookback', cnt.cnt,
      'newest_in_window', cnt.mx,
      'lookback_hours', p_lookback_hours
    )
  FROM (
    SELECT
      COUNT(*) FILTER (
        WHERE observed_at >= (now() - (p_lookback_hours || ' hours')::interval)
      )::bigint AS cnt,
      MAX(observed_at) FILTER (
        WHERE observed_at >= (now() - (p_lookback_hours || ' hours')::interval)
      ) AS mx
    FROM normalized.wind_observations
  ) cnt
  WHERE cnt.cnt = 0

  UNION ALL

  SELECT
    'high_plume_exposure'::text,
    CASE
      WHEN hp.max_score >= LEAST(100::double precision, p_high_plume_exposure_min + 15::double precision)
        THEN 'critical'
      ELSE 'warn'
    END,
    hp.geography_type::text,
    hp.geoid::text,
    format('Elevated wind corridor plume exposure (max score %s)', ROUND(hp.max_score::numeric, 1))::text,
    format(
      'geoid=%s max_score=%s detections=%s window_end=%s',
      hp.geoid,
      hp.max_score,
      hp.detection_count,
      hp.window_end
    ),
    hp.computed_at,
    jsonb_build_object(
      'max_exposure_score', hp.max_score,
      'plume_detection_count', hp.detection_count,
      'window_end', hp.window_end,
      'model_version', hp.model_version
    )
  FROM (
    SELECT
      p.geography_type,
      p.geoid,
      MAX(p.exposure_score) AS max_score,
      COUNT(DISTINCT p.detection_id)::bigint AS detection_count,
      MAX(p.computed_at) AS computed_at,
      MAX(p.window_end) AS window_end,
      MAX(p.model_version) AS model_version
    FROM analytics.smoke_plume_exposures p
    WHERE p.computed_at >= (now() - (p_lookback_hours || ' hours')::interval)
    GROUP BY p.geography_type, p.geoid
  ) hp
  WHERE hp.max_score >= p_high_plume_exposure_min;
$$;

CREATE OR REPLACE VIEW analytics.v_alert_candidates AS
SELECT *
FROM analytics.fn_alert_candidates(6, 24, 75::double precision, 24, 70::double precision);

COMMENT ON VIEW analytics.v_alert_candidates IS
  'Default-parameter alert union; override thresholds via analytics.fn_alert_candidates(...) or scripts/check_alerts.sh.';
