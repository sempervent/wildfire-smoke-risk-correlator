# Kafka lag high (`kafka_lag_high`)

## Meaning

Alert candidates compare **broker high watermarks** (stored in `analytics.kafka_topic_offsets` via `scripts/collect_kafka_lag.sh`) against **application-recorded offsets** in `analytics.kafka_consumer_offsets` for `spark-normalize%` consumer groups. This is **application-observed lag**, not necessarily identical to Kafka consumer-group commit lag.

## Confirm

1. Run `make collect-lag` (or `make kafka-lag`) and inspect `analytics.v_consumer_lag_latest` and `analytics.v_pipeline_lag_summary`.
2. Compare with Spark normalizer logs and Redpanda/Kafka Console for the same topic/partition.
3. Check whether normalizers are stalled, failing, or simply behind on large backlogs.

## Mitigate

- Restart or scale Spark normalization jobs if they are crashed or wedged.
- Investigate poison messages (see parse-error / DLQ runbooks) if one partition never advances.
- Temporarily raise thresholds via `ALERT_KAFKA_LAG_WARN_MESSAGES` / `ALERT_KAFKA_LAG_CRITICAL_MESSAGES` only after confirming sustained backlog is acceptable.

## Related

- `docs/runbooks/parser-failure-spike.md` — upstream parse failures can prevent commits.
- `docs/runbooks/consumer-offset-stale.md` — complementary application offset evidence.
