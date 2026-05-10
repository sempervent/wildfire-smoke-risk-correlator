# DLQ depth high (`dlq_depth_high`)

## Meaning

The alert uses **`analytics.v_dlq_topic_depth`**, which sums **partition high_watermark** snapshots per DLQ topic as a **rough proxy** for retained messages. It is **not** an exact count of unconsumed DLQ records. Open `analytics.parse_errors` rows for the paired raw topic are surfaced for mismatch triage.

## Confirm

1. Query `analytics.v_dlq_topic_depth` and `analytics.v_dlq_operational_summary`.
2. Inspect DLQ topics in Redpanda/Kafka Console (`firms.hotspots.dlq`, `openaq.measurements.dlq`, `weather.wind.dlq`, `normalization.errors`).
3. Correlate with `analytics.v_parse_errors_open` and recent `parser_failure_spike` / `parse_errors_high` candidates.

## Mitigate

- Fix upstream publishers or schemas causing normalization failures.
- Use `make replay-dlq` with **`DRY_RUN=1`** first; republish only validated payloads (`DRY_RUN=0`), optionally `DLQ_RESOLVE_ON_REPLAY=1` for Postgres-backed rows.
- Tune `ALERT_DLQ_DEPTH_WARN_MESSAGES` / `ALERT_DLQ_DEPTH_CRITICAL_MESSAGES` after validating proxy behavior on your cluster.

## Related

- `docs/runbooks/dlq-records-present.md`
- `docs/runbooks/replay-failures-recent.md`
