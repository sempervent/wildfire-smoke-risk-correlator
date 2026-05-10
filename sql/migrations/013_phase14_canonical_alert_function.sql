-- Phase 14: canonical single overload for analytics.fn_alert_candidates.
-- Old dev volumes could accumulate multiple CREATE FUNCTION overloads (signature drift),
-- causing errors like: function analytics.fn_alert_candidates(integer, ...) is not unique.
-- This migration drops every overload via pg_catalog, then installs exactly one 23-parameter
-- SQL function and the default-threshold analytics.v_alert_candidates view.
--
-- MUST run after sql/views that define objects referenced by the function body
-- (e.g. analytics.v_integration_pipeline_counts, analytics.v_dispersion_operational_summary,
-- analytics.v_model_overprediction_candidates). bootstrap_db.sh applies this file last.

DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT p.oid,
           pg_get_function_identity_arguments(p.oid) AS args
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'analytics'
      AND p.proname = 'fn_alert_candidates'
      AND p.prokind = 'f'
  LOOP
    EXECUTE format('DROP FUNCTION IF EXISTS analytics.fn_alert_candidates(%s) CASCADE', r.args);
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION analytics.fn_alert_candidates(
  p_warn_hours integer DEFAULT 6,
  p_crit_hours integer DEFAULT 24,
  p_high_risk_min double precision DEFAULT 75,
  p_lookback_hours integer DEFAULT 24,
  p_high_plume_exposure_min double precision DEFAULT 70,
  p_parse_errors_warn_count integer DEFAULT 1,
  p_parse_errors_critical_count integer DEFAULT 25,
  p_consumer_offset_stale_hours integer DEFAULT 6,
  p_parser_spike_warn_count integer DEFAULT 15,
  p_parser_spike_critical_count integer DEFAULT 40,
  p_kafka_lag_warn_messages bigint DEFAULT 100,
  p_kafka_lag_critical_messages bigint DEFAULT 1000,
  p_dlq_depth_warn_messages bigint DEFAULT 1,
  p_dlq_depth_critical_messages bigint DEFAULT 100,
  p_grid_weather_stale_hours integer DEFAULT 6,
  p_fire_weather_unmatched_warn integer DEFAULT 5,
  p_fire_weather_unmatched_critical integer DEFAULT 25,
  p_high_dispersion_exposure_min double precision DEFAULT 70,
  p_dispersion_no_wind_hours integer DEFAULT 24,
  p_dispersion_aq_mismatch_min double precision DEFAULT 50,
  p_model_mismatch_min_count integer DEFAULT 3,
  p_aq_obs_coverage_min integer DEFAULT 3,
  p_calibration_warn_only integer DEFAULT 1
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
  WHERE hp.max_score >= p_high_plume_exposure_min

  UNION ALL

  SELECT
    'parse_errors_high'::text,
    CASE
      WHEN pe.open_cnt >= p_parse_errors_critical_count THEN 'critical'
      WHEN pe.open_cnt >= p_parse_errors_warn_count THEN 'warn'
      ELSE 'warn'
    END,
    NULL,
    NULL,
    'Elevated open normalization parse errors'::text,
    format('open_parse_errors=%s', pe.open_cnt),
    now(),
    jsonb_build_object(
      'open_parse_errors', pe.open_cnt,
      'warn_threshold', p_parse_errors_warn_count,
      'critical_threshold', p_parse_errors_critical_count
    )
  FROM (
    SELECT COUNT(*)::bigint AS open_cnt
    FROM analytics.parse_errors
    WHERE status = 'open'
  ) pe
  WHERE pe.open_cnt >= p_parse_errors_warn_count

  UNION ALL

  SELECT
    'parser_failure_spike'::text,
    CASE
      WHEN sp.spike_sum >= p_parser_spike_critical_count THEN 'critical'
      WHEN sp.spike_sum >= p_parser_spike_warn_count THEN 'warn'
      ELSE 'warn'
    END,
    NULL,
    NULL,
    'Parser failure spike (last hour)'::text,
    format('occurrence_sum_1h=%s', sp.spike_sum),
    now(),
    jsonb_build_object(
      'occurrence_sum_1h', sp.spike_sum,
      'warn_threshold', p_parser_spike_warn_count,
      'critical_threshold', p_parser_spike_critical_count
    )
  FROM (
    SELECT COALESCE(SUM(occurrence_count), 0)::bigint AS spike_sum
    FROM analytics.parse_errors
    WHERE last_seen_at >= (now() - interval '1 hour')
  ) sp
  WHERE sp.spike_sum >= p_parser_spike_warn_count

  UNION ALL

  SELECT
    'dlq_records_present'::text,
    'warn'::text,
    NULL,
    NULL,
    'Recent parse errors touching DLQ pipeline'::text,
    format('open_parse_errors_last_15m=%s', dq.cnt),
    now(),
    jsonb_build_object(
      'open_recent_15m', dq.cnt,
      'hint', 'Inspect analytics.v_parse_errors_recent and DLQ Kafka topics'
    )
  FROM (
    SELECT COUNT(*)::bigint AS cnt
    FROM analytics.parse_errors
    WHERE status = 'open'
      AND last_seen_at >= (now() - interval '15 minutes')
  ) dq
  WHERE dq.cnt >= 1

  UNION ALL

  SELECT
    'consumer_offset_stale'::text,
    CASE
      WHEN cos.no_evidence THEN 'warn'
      ELSE 'critical'
    END,
    NULL,
    NULL,
    'Stale or missing Spark normalizer offset evidence'::text,
    format(
      'rows=%s worst_age_hours=%s',
      cos.row_cnt,
      ROUND(cos.worst_age::numeric, 2)
    ),
    now(),
    jsonb_build_object(
      'offset_rows', cos.row_cnt,
      'worst_age_hours', cos.worst_age,
      'stale_after_hours', p_consumer_offset_stale_hours,
      'no_evidence', cos.no_evidence
    )
  FROM (
    SELECT
      COUNT(*)::bigint AS row_cnt,
      COALESCE(
        MAX(EXTRACT(epoch FROM (now() - last_processed_at)) / 3600.0),
        0::double precision
      ) AS worst_age,
      (COUNT(*) = 0) AS no_evidence
    FROM analytics.kafka_consumer_offsets
    WHERE consumer_group LIKE 'spark-normalize%'
  ) cos
  WHERE cos.no_evidence
     OR cos.worst_age >= (p_consumer_offset_stale_hours::double precision)

  UNION ALL

  SELECT
    'kafka_lag_high'::text,
    CASE
      WHEN kl.total >= p_kafka_lag_critical_messages THEN 'critical'
      WHEN kl.total >= p_kafka_lag_warn_messages THEN 'warn'
      ELSE 'warn'
    END,
    NULL,
    NULL,
    'Elevated Kafka consumer lag (application-observed)'::text,
    format('total_lag_messages=%s', kl.total),
    now(),
    jsonb_build_object(
      'total_lag_messages', kl.total,
      'warn_threshold', p_kafka_lag_warn_messages,
      'critical_threshold', p_kafka_lag_critical_messages,
      'hint', 'See analytics.v_consumer_lag_latest and analytics.kafka_consumer_lag_observations'
    )
  FROM (
    SELECT COALESCE(SUM(lag), 0)::bigint AS total
    FROM analytics.v_consumer_lag_latest
    WHERE consumer_group LIKE 'spark-normalize%'
      AND topic IN (
        'firms.hotspots.raw',
        'openaq.measurements.raw',
        'weather.wind.raw'
      )
  ) kl
  WHERE kl.total >= p_kafka_lag_warn_messages

  UNION ALL

  SELECT
    'dlq_depth_high'::text,
    CASE
      WHEN dq.total >= p_dlq_depth_critical_messages THEN 'critical'
      WHEN dq.total >= p_dlq_depth_warn_messages THEN 'warn'
      ELSE 'warn'
    END,
    NULL,
    NULL,
    'Elevated DLQ topic depth proxy'::text,
    format('approx_dlq_messages_proxy_sum=%s', dq.total),
    now(),
    jsonb_build_object(
      'approx_dlq_messages_proxy_sum', dq.total,
      'warn_threshold', p_dlq_depth_warn_messages,
      'critical_threshold', p_dlq_depth_critical_messages,
      'hint', 'See analytics.v_dlq_topic_depth (proxy only)'
    )
  FROM (
    SELECT COALESCE(SUM(approx_dlq_messages_proxy), 0)::bigint AS total
    FROM analytics.v_dlq_topic_depth
  ) dq
  WHERE dq.total >= p_dlq_depth_warn_messages

  UNION ALL

  SELECT
    'replay_failures_recent'::text,
    'warn'::text,
    NULL,
    NULL,
    'Recent DLQ replay failures'::text,
    format('failed_replay_runs_24h=%s', rf.cnt),
    now(),
    jsonb_build_object(
      'failed_runs', rf.cnt,
      'hint', 'Inspect analytics.v_dlq_replay_runs'
    )
  FROM (
    SELECT COUNT(*)::bigint AS cnt
    FROM analytics.dlq_replay_runs
    WHERE status = 'failed'
      AND started_at >= (now() - interval '24 hours')
  ) rf
  WHERE rf.cnt >= 1

  UNION ALL

  SELECT
    'grid_weather_stale'::text,
    CASE
      WHEN gw.mx IS NULL THEN 'critical'
      WHEN gw.mx < (now() - (p_grid_weather_stale_hours || ' hours')::interval) THEN 'critical'
      ELSE 'warn'
    END,
    NULL::text,
    NULL::text,
    'Stale gridded weather coverage'::text,
    format('MAX(weather_grid_cells.valid_time)=%s', gw.mx),
    COALESCE(gw.mx, now()),
    jsonb_build_object(
      'max_valid_time', gw.mx,
      'stale_hours_threshold', p_grid_weather_stale_hours,
      'hint', 'Inspect normalized.weather_grid_cells and grid ingest jobs.'
    )
  FROM (
    SELECT MAX(valid_time) AS mx
    FROM normalized.weather_grid_cells
  ) gw
  WHERE EXISTS (SELECT 1 FROM normalized.weather_grid_cells LIMIT 1)
    AND (
      gw.mx IS NULL
      OR gw.mx < (now() - (p_grid_weather_stale_hours || ' hours')::interval)
    )

  UNION ALL

  SELECT
    'no_recent_grid_weather'::text,
    'warn'::text,
    NULL::text,
    NULL::text,
    'No recent weather grid cells'::text,
    format('recent_cell_count_24h=%s', rc.cnt),
    now(),
    jsonb_build_object('recent_cell_count_24h', rc.cnt)
  FROM (
    SELECT COUNT(*)::bigint AS cnt
    FROM normalized.weather_grid_cells
    WHERE valid_time >= (now() - interval '24 hours')
  ) rc
  WHERE rc.cnt = 0
    AND EXISTS (SELECT 1 FROM normalized.weather_grid_cells LIMIT 1)

  UNION ALL

  SELECT
    'fire_weather_unmatched_high'::text,
    CASE
      WHEN um.u >= p_fire_weather_unmatched_critical THEN 'critical'
      ELSE 'warn'
    END,
    NULL::text,
    NULL::text,
    'Many recent fires lack grid-weather matches'::text,
    format('unmatched_recent_fires=%s of %s', um.u, rf.total),
    now(),
    jsonb_build_object(
      'unmatched_recent_fires', um.u,
      'recent_fire_rows', rf.total,
      'warn_threshold', p_fire_weather_unmatched_warn,
      'critical_threshold', p_fire_weather_unmatched_critical
    )
  FROM (
    SELECT COUNT(*)::bigint AS total
    FROM normalized.fire_detections
    WHERE acq_datetime >= (now() - interval '24 hours')
  ) rf,
  (
    SELECT COUNT(*)::bigint AS u
    FROM normalized.fire_detections f
    WHERE f.acq_datetime >= (now() - interval '24 hours')
      AND NOT EXISTS (
        SELECT 1 FROM analytics.fire_weather_matches m WHERE m.detection_id = f.detection_id
      )
  ) um
  WHERE rf.total > 0
    AND um.u >= p_fire_weather_unmatched_warn

  UNION ALL

  SELECT
    'grid_weather_parse_errors_high'::text,
    CASE
      WHEN pe.c >= p_parse_errors_critical_count THEN 'critical'
      ELSE 'warn'
    END,
    NULL::text,
    NULL::text,
    'Elevated open grid-weather parse errors'::text,
    format('open_grid_parse_errors=%s', pe.c),
    now(),
    jsonb_build_object(
      'open_parse_errors', pe.c,
      'source_topic', 'weather.grid.raw'
    )
  FROM (
    SELECT COUNT(*)::bigint AS c
    FROM analytics.parse_errors
    WHERE status = 'open'
      AND source_topic = 'weather.grid.raw'
  ) pe
  WHERE pe.c >= p_parse_errors_warn_count

  UNION ALL

  SELECT
    'integration_pipeline_incomplete'::text,
    'warn'::text,
    NULL::text,
    NULL::text,
    'Integration pipeline coverage incomplete'::text,
    format(
      'fires_24h=%s aq_24h=%s wind_24h=%s grid_cells_24h=%s kafka_offsets=%s',
      ipc.fires_24h,
      ipc.aq_24h,
      ipc.wind_24h,
      ipc.grid_cells_24h,
      ipc.kafka_topic_offset_rows
    ),
    now(),
    jsonb_build_object(
      'fires_24h', ipc.fires_24h,
      'aq_24h', ipc.aq_24h,
      'wind_24h', ipc.wind_24h,
      'grid_cells_24h', ipc.grid_cells_24h,
      'kafka_topic_offset_rows', ipc.kafka_topic_offset_rows
    )
  FROM analytics.v_integration_pipeline_counts ipc
  WHERE ipc.fires_24h > 0
    AND (
      ipc.aq_24h = 0
      OR ipc.wind_24h = 0
      OR ipc.grid_cells_24h = 0
      OR ipc.kafka_topic_offset_rows = 0
    )

  UNION ALL

  SELECT
    'v4_risk_missing'::text,
    'warn'::text,
    NULL::text,
    NULL::text,
    'No smoke risk v4 scores materialized'::text,
    format('fires_24h=%s risk_v4_rows=%s', ipc.fires_24h, ipc.risk_v4_rows),
    now(),
    jsonb_build_object('fires_24h', ipc.fires_24h, 'risk_v4_rows', ipc.risk_v4_rows)
  FROM analytics.v_integration_pipeline_counts ipc
  WHERE ipc.fires_24h > 0
    AND ipc.risk_v4_rows = 0

  UNION ALL

  SELECT
    'fire_weather_match_missing'::text,
    'warn'::text,
    NULL::text,
    NULL::text,
    'Fire detections present but no fire–weather matches'::text,
    format(
      'fires_24h=%s grid_cells_24h=%s matches=%s',
      ipc.fires_24h,
      ipc.grid_cells_24h,
      ipc.total_fire_weather_matches
    ),
    now(),
    jsonb_build_object(
      'fires_24h', ipc.fires_24h,
      'grid_cells_24h', ipc.grid_cells_24h,
      'total_fire_weather_matches', ipc.total_fire_weather_matches
    )
  FROM analytics.v_integration_pipeline_counts ipc
  WHERE ipc.fires_24h > 0
    AND ipc.grid_cells_24h > 0
    AND ipc.total_fire_weather_matches = 0

  UNION ALL

  SELECT
    'high_dispersion_exposure'::text,
    'warn'::text,
    NULL::text,
    NULL::text,
    'Elevated Gaussian dispersion proxy score'::text,
    format('max_dispersion_score_24h=%s', dos.max_dispersion_score_24h),
    now(),
    jsonb_build_object(
      'max_dispersion_score_24h', dos.max_dispersion_score_24h,
      'threshold', p_high_dispersion_exposure_min
    )
  FROM analytics.v_dispersion_operational_summary dos
  WHERE dos.max_dispersion_score_24h >= p_high_dispersion_exposure_min

  UNION ALL

  SELECT
    'dispersion_no_wind_matches'::text,
    'warn'::text,
    NULL::text,
    NULL::text,
    'Recent fires lack dispersion rows despite grid-wind matches'::text,
    format('unmatched_fire_count=%s lookback_hours=%s', gap.cnt, p_dispersion_no_wind_hours),
    now(),
    jsonb_build_object(
      'unmatched_fire_count', gap.cnt,
      'lookback_hours', p_dispersion_no_wind_hours,
      'hint', 'Run compute-dispersion with DISPERSION_ENABLED=1'
    )
  FROM (
    SELECT COUNT(*)::bigint AS cnt
    FROM normalized.fire_detections f
    WHERE f.acq_datetime >= now() - interval '24 hours'
      AND EXISTS (SELECT 1 FROM analytics.fire_weather_matches m WHERE m.detection_id = f.detection_id)
      AND NOT EXISTS (
        SELECT 1 FROM analytics.smoke_dispersion_exposures e
        WHERE e.detection_id = f.detection_id
          AND e.computed_at >= now() - (p_dispersion_no_wind_hours || ' hours')::interval
      )
  ) gap
  WHERE gap.cnt >= 3

  UNION ALL

  SELECT
    'dispersion_no_targets'::text,
    'warn'::text,
    NULL::text,
    NULL::text,
    'Dispersion rows present but no positive scores recently'::text,
    format(
      'dispersion_rows_24h=%s dispersion_positive_scores_24h=%s fires_24h=%s',
      dos.dispersion_rows_24h,
      dos.dispersion_positive_scores_24h,
      dos.fires_24h
    ),
    now(),
    jsonb_build_object(
      'dispersion_rows_24h', dos.dispersion_rows_24h,
      'dispersion_positive_scores_24h', dos.dispersion_positive_scores_24h,
      'fires_24h', dos.fires_24h
    )
  FROM analytics.v_dispersion_operational_summary dos
  WHERE dos.dispersion_rows_24h > 0
    AND dos.dispersion_positive_scores_24h = 0
    AND dos.fires_24h > 0

  UNION ALL

  SELECT
    'dispersion_aq_mismatch_high'::text,
    'warn'::text,
    NULL::text,
    NULL::text,
    'Dispersion vs AQ lag comparison divergence'::text,
    format('high_mismatch_rows=%s', mx.cnt),
    now(),
    jsonb_build_object('high_mismatch_rows', mx.cnt, 'threshold', p_dispersion_aq_mismatch_min)
  FROM (
    SELECT COUNT(*)::bigint AS cnt
    FROM analytics.dispersion_aq_comparisons c
    WHERE c.comparison_score >= p_dispersion_aq_mismatch_min
      AND c.aq_observation_count >= 2
      AND c.computed_at >= (now() - interval '48 hours')
  ) mx
  WHERE mx.cnt >= 1

  UNION ALL

  SELECT
    'model_overprediction_possible'::text,
    CASE WHEN p_calibration_warn_only <> 0 THEN 'info'::text ELSE 'warn'::text END,
    NULL::text,
    NULL::text,
    'Model vs AQ: possible overprediction (heuristic)'::text,
    format('candidate_rows=%s (min %s)', oc.cnt, p_model_mismatch_min_count),
    now(),
    jsonb_build_object('candidate_rows', oc.cnt, 'threshold', p_model_mismatch_min_count)
  FROM (
    SELECT COUNT(*)::bigint AS cnt
    FROM analytics.v_model_overprediction_candidates
  ) oc
  WHERE oc.cnt >= p_model_mismatch_min_count::bigint

  UNION ALL

  SELECT
    'model_underprediction_possible'::text,
    CASE WHEN p_calibration_warn_only <> 0 THEN 'info'::text ELSE 'warn'::text END,
    NULL::text,
    NULL::text,
    'Model vs AQ: possible underprediction (heuristic)'::text,
    format('candidate_rows=%s (min %s)', uc.cnt, p_model_mismatch_min_count),
    now(),
    jsonb_build_object('candidate_rows', uc.cnt, 'threshold', p_model_mismatch_min_count)
  FROM (
    SELECT COUNT(*)::bigint AS cnt
    FROM analytics.v_model_underprediction_candidates
  ) uc
  WHERE uc.cnt >= p_model_mismatch_min_count::bigint

  UNION ALL

  SELECT
    'calibration_insufficient_data'::text,
    CASE WHEN p_calibration_warn_only <> 0 THEN 'info'::text ELSE 'warn'::text END,
    NULL::text,
    NULL::text,
    'Calibration comparisons dominated by missing/thin AQ windows'::text,
    format(
      'thin_rows=%s total_rows=%s (>=70%% thin; min_total=%s)',
      s.thin,
      s.tot,
      p_model_mismatch_min_count
    ),
    now(),
    jsonb_build_object(
      'thin_rows', s.thin,
      'total_rows', s.tot,
      'threshold_min_total', p_model_mismatch_min_count
    )
  FROM (
    SELECT
      COUNT(*)::bigint AS tot,
      SUM(
        CASE WHEN evidence_label IN ('no_aq_data', 'insufficient_aq_data') THEN 1 ELSE 0 END
      )::bigint AS thin
    FROM analytics.dispersion_aq_comparisons
    WHERE computed_at >= (now() - INTERVAL '72 hours')
  ) s
  WHERE s.tot >= p_model_mismatch_min_count::bigint
    AND s.tot > 0
    AND (s.thin::double precision / s.tot::double precision) >= 0.7

  UNION ALL

  SELECT
    'aq_observation_coverage_low'::text,
    CASE WHEN p_calibration_warn_only <> 0 THEN 'info'::text ELSE 'warn'::text END,
    NULL::text,
    NULL::text,
    'Distinct tract AQ coverage in last 24h below threshold'::text,
    format(
      'distinct_tract_aq_geographies=%s (min %s); fires_24h=%s',
      aq.aq_geo,
      p_aq_obs_coverage_min,
      fc.fc
    ),
    now(),
    jsonb_build_object(
      'distinct_tract_aq_geographies', aq.aq_geo,
      'threshold', p_aq_obs_coverage_min,
      'fires_24h', fc.fc
    )
  FROM (
    SELECT COUNT(DISTINCT tract_geoid)::bigint AS aq_geo
    FROM normalized.air_quality_measurements
    WHERE measured_at >= (now() - INTERVAL '24 hours')
      AND tract_geoid IS NOT NULL
  ) aq,
  (
    SELECT COUNT(*)::bigint AS fc
    FROM normalized.fire_detections
    WHERE acq_datetime >= (now() - INTERVAL '24 hours')
  ) fc
  WHERE fc.fc > 0
    AND aq.aq_geo < p_aq_obs_coverage_min::bigint
$$;

CREATE OR REPLACE VIEW analytics.v_alert_candidates AS
SELECT *
FROM analytics.fn_alert_candidates(
  6,
  24,
  75::double precision,
  24,
  70::double precision,
  1,
  25,
  6,
  15,
  40,
  100::bigint,
  1000::bigint,
  1::bigint,
  100::bigint,
  6,
  5,
  25,
  70::double precision,
  24,
  50::double precision,
  3,
  3,
  1
);

COMMENT ON VIEW analytics.v_alert_candidates IS
  'Default-parameter alert union (integration + calibration SLIs); override thresholds via analytics.fn_alert_candidates(...) or scripts/check_alerts.sh.';
