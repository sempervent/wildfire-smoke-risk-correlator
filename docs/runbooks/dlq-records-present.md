# Runbook: DLQ / recent parse errors (`dlq_records_present`)

## Meaning

At least one **open** parse error row had **`last_seen_at`** within the last **15 minutes**—signals active normalization failures or replay churn.

## Confirm

```sql
SELECT * FROM analytics.v_parse_errors_recent LIMIT 100;
```

Consume a few messages from **`normalization.errors`** or the matching source DLQ topic (no secrets in envelopes).

## Mitigate

- Treat as an early signal: triage **`error_message`** and **`error_context`** JSON.
- Dry-run **`scripts/replay_dlq.sh`** (`DRY_RUN=1` default) before any republish.
- Resolve **`parse_errors`** only after confirming payloads are valid (**`DLQ_RESOLVE_ON_REPLAY=1`** optional on actual replay).

## Escalate

If DLQ depth grows without matching Postgres rows, check Spark connectivity to Postgres/Kafka on the executor path.
