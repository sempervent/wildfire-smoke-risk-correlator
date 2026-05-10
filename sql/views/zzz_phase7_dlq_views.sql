-- Phase 7: parse error + consumer offset observability.

CREATE OR REPLACE VIEW analytics.v_parse_errors_open AS
SELECT
  parse_error_id,
  source_topic,
  target_dataset,
  consumer_group,
  partition,
  offset_value,
  error_class,
  LEFT(error_message, 240) AS error_message_preview,
  occurrence_count,
  first_seen_at,
  last_seen_at,
  payload_hash,
  payload_sample,
  status,
  'replay: scripts/replay_dlq.sh (DRY_RUN=1 default)'::text AS replay_hint
FROM analytics.parse_errors
WHERE status = 'open'
ORDER BY last_seen_at DESC NULLS LAST;

CREATE OR REPLACE VIEW analytics.v_parse_error_summary AS
SELECT
  source_topic,
  target_dataset,
  error_class,
  status,
  COUNT(*)::bigint AS error_rows,
  SUM(occurrence_count)::bigint AS total_occurrences,
  MAX(last_seen_at) AS latest_last_seen_at,
  MIN(first_seen_at) AS earliest_first_seen_at
FROM analytics.parse_errors
GROUP BY source_topic, target_dataset, error_class, status
ORDER BY latest_last_seen_at DESC NULLS LAST;

CREATE OR REPLACE VIEW analytics.v_parse_errors_recent AS
SELECT *
FROM (
  SELECT *
  FROM analytics.parse_errors
  WHERE last_seen_at >= (now() - interval '24 hours')
  ORDER BY last_seen_at DESC NULLS LAST
  LIMIT 500
) s;

CREATE OR REPLACE VIEW analytics.v_consumer_offset_state AS
SELECT
  consumer_group,
  topic,
  partition,
  current_offset,
  last_processed_at,
  last_successful_offset,
  last_error_offset,
  EXTRACT(epoch FROM (now() - last_processed_at)) / 3600.0 AS age_hours,
  metadata
FROM analytics.kafka_consumer_offsets
WHERE consumer_group LIKE 'spark-normalize%'
ORDER BY topic, partition;

CREATE OR REPLACE VIEW analytics.v_dlq_operational_summary AS
SELECT
  (SELECT COUNT(*)::bigint FROM analytics.parse_errors WHERE status = 'open') AS open_parse_errors,
  (
    SELECT COUNT(*)::bigint
    FROM analytics.parse_errors
    WHERE last_seen_at >= (now() - interval '24 hours')
  ) AS parse_errors_last_24h,
  (
    SELECT COALESCE(MAX(last_seen_at), to_timestamp(0))
    FROM analytics.parse_errors
  ) AS newest_parse_error_seen_at,
  (
    SELECT COUNT(*)::bigint
    FROM analytics.kafka_consumer_offsets
    WHERE consumer_group LIKE 'spark-normalize%'
  ) AS consumer_offset_partitions_tracked,
  (
    SELECT COALESCE(
      MAX(EXTRACT(epoch FROM (now() - last_processed_at)) / 3600.0),
      NULL
    )
    FROM analytics.kafka_consumer_offsets
    WHERE consumer_group LIKE 'spark-normalize%'
  ) AS worst_consumer_offset_age_hours;

COMMENT ON VIEW analytics.v_parse_errors_open IS 'Operator-facing open quarantine rows with replay hint.';
COMMENT ON VIEW analytics.v_dlq_operational_summary IS 'Single-row coarse counters for Grafana parse/DLQ health (Kafka DLQ depth requires brokers/metrics).';
