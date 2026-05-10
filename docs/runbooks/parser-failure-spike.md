# Runbook: parser failure spike (`parser_failure_spike`)

## Meaning

Summed **`occurrence_count`** on **`analytics.parse_errors`** with **`last_seen_at`** in the last hour crossed configured floors inside **`analytics.fn_alert_candidates`** (warn ≥ 15 occurrences sum, critical ≥ 40).

## Confirm

```sql
SELECT source_topic, target_dataset, error_class, SUM(occurrence_count) AS occ
FROM analytics.parse_errors
WHERE last_seen_at >= now() - interval '1 hour'
GROUP BY 1, 2, 3
ORDER BY occ DESC;
```

## Mitigate

- Compare failing **`payload_hash`** / **`payload_sample`** against producer fixtures.
- Roll back bad deployments or pause ingestion if a single bad template is fanning out.
- After fixes, **`make replay-bad-fixtures`** (optional) + **`make normalize`** in dev to verify quarantine behavior.

## Escalate

If spikes correlate with broker lag, capture Spark executor logs for the relevant **`NORMALIZER_CONSUMER_GROUP`**.
