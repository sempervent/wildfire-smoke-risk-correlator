-- Phase 8: broker depth, lag projections, DLQ depth, replay bookkeeping views.

CREATE OR REPLACE VIEW analytics.v_kafka_topic_depth AS
SELECT DISTINCT ON (topic, partition)
  topic,
  partition,
  low_watermark,
  high_watermark,
  observed_at AS depth_observed_at,
  COALESCE(high_watermark, 0) - COALESCE(low_watermark, 0) AS approx_span_messages,
  metadata
FROM analytics.kafka_topic_offsets
ORDER BY topic, partition, observed_at DESC NULLS LAST;

COMMENT ON VIEW analytics.v_kafka_topic_depth IS 'Latest broker watermark snapshot per topic/partition.';

CREATE OR REPLACE VIEW analytics.v_consumer_lag_latest AS
SELECT DISTINCT ON (consumer_group, topic, partition)
  consumer_group,
  topic,
  partition,
  current_offset,
  high_watermark,
  lag,
  observed_at AS lag_observed_at,
  metadata
FROM analytics.kafka_consumer_lag_observations
ORDER BY consumer_group, topic, partition, observed_at DESC NULLS LAST;

COMMENT ON VIEW analytics.v_consumer_lag_latest IS 'Latest lag observation per consumer group/topic/partition.';

CREATE OR REPLACE VIEW analytics.v_dlq_topic_depth AS
WITH dlq_topics AS (
  SELECT * FROM (VALUES
    ('firms.hotspots.dlq'::text, 'firms.hotspots.raw'::text),
    ('openaq.measurements.dlq', 'openaq.measurements.raw'),
    ('weather.wind.dlq', 'weather.wind.raw'),
    ('normalization.errors', NULL::text)
  ) AS m(dlq_topic, raw_topic)
)
SELECT
  d.dlq_topic,
  COALESCE(SUM(k.high_watermark), 0)::bigint AS approx_dlq_messages_proxy,
  MAX(k.depth_observed_at) AS newest_observed_at,
  (
    SELECT COUNT(*)::bigint
    FROM analytics.parse_errors pe
    WHERE pe.status = 'open'
      AND d.raw_topic IS NOT NULL
      AND pe.source_topic = d.raw_topic
  ) AS open_parse_errors_for_raw_topic,
  'Sum of partition high_watermark snapshots (rough proxy, not exact queued DLQ messages).'::text AS depth_hint
FROM dlq_topics d
LEFT JOIN analytics.v_kafka_topic_depth k ON k.topic = d.dlq_topic
GROUP BY d.dlq_topic, d.raw_topic;

COMMENT ON VIEW analytics.v_dlq_topic_depth IS 'Coarse DLQ depth proxy plus open parse_errors counts for mismatch triage.';

CREATE OR REPLACE VIEW analytics.v_dlq_replay_candidates AS
SELECT
  parse_error_id,
  source_topic,
  target_dataset,
  consumer_group,
  error_class,
  occurrence_count,
  last_seen_at,
  status,
  payload_hash,
  'replay: scripts/replay_dlq.sh (DRY_RUN=1 default)'::text AS replay_hint
FROM analytics.parse_errors
WHERE status = 'open'
ORDER BY last_seen_at DESC NULLS LAST;

COMMENT ON VIEW analytics.v_dlq_replay_candidates IS 'Open quarantine rows suitable for operator replay review.';

CREATE OR REPLACE VIEW analytics.v_dlq_replay_runs AS
SELECT
  dlq_replay_run_id,
  source,
  source_topic,
  target_dataset,
  status,
  dry_run,
  started_at,
  finished_at,
  records_scanned,
  records_replayed,
  records_resolved,
  error_message,
  config
FROM analytics.dlq_replay_runs
ORDER BY started_at DESC NULLS LAST;

CREATE OR REPLACE VIEW analytics.v_dlq_replay_items_recent AS
SELECT
  i.dlq_replay_item_id,
  i.dlq_replay_run_id,
  i.parse_error_id,
  i.source_topic,
  i.target_topic,
  i.payload_hash,
  i.status,
  LEFT(i.error_message, 400) AS error_message_preview,
  i.created_at,
  r.dry_run AS run_dry_run,
  r.source AS run_source
FROM analytics.dlq_replay_items i
JOIN analytics.dlq_replay_runs r ON r.dlq_replay_run_id = i.dlq_replay_run_id
WHERE i.created_at >= (now() - interval '7 days')
ORDER BY i.created_at DESC NULLS LAST;

CREATE OR REPLACE VIEW analytics.v_pipeline_lag_summary AS
WITH raw_lag AS (
  SELECT COALESCE(SUM(lag), 0)::bigint AS total_raw_lag
  FROM analytics.v_consumer_lag_latest
  WHERE consumer_group LIKE 'spark-normalize%'
    AND topic IN (
      'firms.hotspots.raw',
      'openaq.measurements.raw',
      'weather.wind.raw'
    )
),
dlq_sum AS (
  SELECT COALESCE(SUM(approx_dlq_messages_proxy), 0)::bigint AS total_dlq_proxy
  FROM analytics.v_dlq_topic_depth
),
pe AS (
  SELECT COUNT(*) FILTER (WHERE status = 'open')::bigint AS open_parse_errors
  FROM analytics.parse_errors
),
topic_rows AS (
  SELECT COUNT(DISTINCT (topic, partition))::bigint AS topic_partitions_sampled
  FROM analytics.v_kafka_topic_depth
)
SELECT
  raw_lag.total_raw_lag,
  dlq_sum.total_dlq_proxy,
  pe.open_parse_errors,
  topic_rows.topic_partitions_sampled,
  now() AS summary_generated_at,
  jsonb_build_object(
    'note',
    'total_raw_lag sums v_consumer_lag_latest.lag for spark-normalize% on raw topics; DLQ proxy is indicative only.'
  ) AS hints
FROM raw_lag, dlq_sum, pe, topic_rows;

COMMENT ON VIEW analytics.v_pipeline_lag_summary IS 'Single-row coarse pipeline lag + DLQ proxy + parse error counts.';
