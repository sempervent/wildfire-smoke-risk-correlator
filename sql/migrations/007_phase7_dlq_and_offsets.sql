-- Phase 7: parse error quarantine, DLQ observability, consumer offset evidence.

CREATE TABLE IF NOT EXISTS analytics.parse_errors (
    parse_error_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_topic text NOT NULL,
    target_dataset text NOT NULL,
    consumer_group text NOT NULL,
    partition integer,
    offset_value bigint,
    message_key text,
    payload_hash text NOT NULL,
    payload_sample jsonb,
    error_class text NOT NULL,
    error_message text NOT NULL,
    error_context jsonb NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    occurrence_count integer NOT NULL DEFAULT 1,
    status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'ignored', 'resolved')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS parse_errors_open_logical_uidx
    ON analytics.parse_errors (source_topic, target_dataset, consumer_group, payload_hash, error_class)
    WHERE status = 'open';

CREATE INDEX IF NOT EXISTS parse_errors_source_topic_idx ON analytics.parse_errors (source_topic);
CREATE INDEX IF NOT EXISTS parse_errors_target_dataset_idx ON analytics.parse_errors (target_dataset);
CREATE INDEX IF NOT EXISTS parse_errors_status_idx ON analytics.parse_errors (status);
CREATE INDEX IF NOT EXISTS parse_errors_last_seen_at_idx ON analytics.parse_errors (last_seen_at DESC);
CREATE INDEX IF NOT EXISTS parse_errors_payload_hash_idx ON analytics.parse_errors (payload_hash);

COMMENT ON TABLE analytics.parse_errors IS 'Quarantined normalization parse failures; DLQ Kafka topics carry full payloads for replay.';

CREATE TABLE IF NOT EXISTS analytics.kafka_consumer_offsets (
    consumer_group text NOT NULL,
    topic text NOT NULL,
    partition integer NOT NULL,
    current_offset bigint NOT NULL,
    last_processed_at timestamptz NOT NULL DEFAULT now(),
    last_successful_offset bigint,
    last_error_offset bigint,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (consumer_group, topic, partition)
);

CREATE INDEX IF NOT EXISTS kafka_consumer_offsets_topic_idx ON analytics.kafka_consumer_offsets (topic);
CREATE INDEX IF NOT EXISTS kafka_consumer_offsets_group_idx ON analytics.kafka_consumer_offsets (consumer_group);

COMMENT ON TABLE analytics.kafka_consumer_offsets IS 'Application evidence of last processed Kafka offsets per Spark batch normalizer (distinct from broker-internal committed offsets).';
