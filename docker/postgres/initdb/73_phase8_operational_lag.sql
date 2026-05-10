-- Phase 8: broker lag evidence, DLQ replay bookkeeping, optional parse_errors archival.

DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT c.conname AS conname
    FROM pg_constraint c
    JOIN pg_class rel ON rel.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = rel.relnamespace
    WHERE n.nspname = 'analytics'
      AND rel.relname = 'parse_errors'
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) LIKE '%status%'
  LOOP
    EXECUTE format('ALTER TABLE analytics.parse_errors DROP CONSTRAINT %I', r.conname);
  END LOOP;
END $$;

ALTER TABLE analytics.parse_errors
  ADD CONSTRAINT parse_errors_status_check
  CHECK (status IN ('open', 'ignored', 'resolved', 'archived'));

CREATE TABLE IF NOT EXISTS analytics.kafka_topic_offsets (
    topic text NOT NULL,
    partition integer NOT NULL,
    low_watermark bigint,
    high_watermark bigint,
    observed_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (topic, partition, observed_at)
);

CREATE INDEX IF NOT EXISTS kafka_topic_offsets_topic_idx ON analytics.kafka_topic_offsets (topic);
CREATE INDEX IF NOT EXISTS kafka_topic_offsets_observed_at_idx ON analytics.kafka_topic_offsets (observed_at DESC);

COMMENT ON TABLE analytics.kafka_topic_offsets IS 'Broker-observed partition watermarks over time (distinct from analytics.kafka_consumer_offsets application evidence).';

CREATE TABLE IF NOT EXISTS analytics.kafka_consumer_lag_observations (
    consumer_group text NOT NULL,
    topic text NOT NULL,
    partition integer NOT NULL,
    current_offset bigint,
    high_watermark bigint,
    lag bigint,
    observed_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS kafka_consumer_lag_obs_topic_idx ON analytics.kafka_consumer_lag_observations (topic);
CREATE INDEX IF NOT EXISTS kafka_consumer_lag_obs_group_topic_idx ON analytics.kafka_consumer_lag_observations (consumer_group, topic);
CREATE INDEX IF NOT EXISTS kafka_consumer_lag_obs_observed_at_idx ON analytics.kafka_consumer_lag_observations (observed_at DESC);
CREATE INDEX IF NOT EXISTS kafka_consumer_lag_obs_lag_idx ON analytics.kafka_consumer_lag_observations (lag DESC NULLS LAST);

COMMENT ON TABLE analytics.kafka_consumer_lag_observations IS 'Lag snapshots (broker high watermark vs committed/application offset); semantics documented in kafka_lag collector.';

CREATE TABLE IF NOT EXISTS analytics.dlq_replay_runs (
    dlq_replay_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source text NOT NULL CHECK (source IN ('postgres', 'kafka')),
    source_topic text,
    target_dataset text,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    dry_run boolean NOT NULL DEFAULT true,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    records_scanned integer NOT NULL DEFAULT 0,
    records_replayed integer NOT NULL DEFAULT 0,
    records_resolved integer NOT NULL DEFAULT 0,
    error_message text,
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dlq_replay_runs_started_at_idx ON analytics.dlq_replay_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS dlq_replay_runs_status_idx ON analytics.dlq_replay_runs (status);

CREATE TABLE IF NOT EXISTS analytics.dlq_replay_items (
    dlq_replay_item_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dlq_replay_run_id uuid NOT NULL REFERENCES analytics.dlq_replay_runs (dlq_replay_run_id) ON DELETE CASCADE,
    parse_error_id uuid REFERENCES analytics.parse_errors (parse_error_id),
    source_topic text,
    target_topic text,
    payload_hash text,
    status text NOT NULL CHECK (status IN ('planned', 'replayed', 'skipped', 'failed')),
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dlq_replay_items_run_idx ON analytics.dlq_replay_items (dlq_replay_run_id);
CREATE INDEX IF NOT EXISTS dlq_replay_items_created_idx ON analytics.dlq_replay_items (created_at DESC);
