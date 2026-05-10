# Runbook: elevated open parse errors (`parse_errors_high`)

## Meaning

The count of **`analytics.parse_errors`** rows with **`status = 'open'`** reached the warn or critical threshold (`ALERT_PARSE_ERRORS_WARN_COUNT` / `ALERT_PARSE_ERRORS_CRITICAL_COUNT`).

## Confirm

```sql
SELECT * FROM analytics.v_parse_error_summary ORDER BY latest_last_seen_at DESC;
SELECT * FROM analytics.v_parse_errors_open LIMIT 50;
```

Check **`normalization.errors`** and source DLQs (`firms.hotspots.dlq`, `openaq.measurements.dlq`, `weather.wind.dlq`) for recent envelopes.

## Mitigate

- Identify **`error_class`** / **`source_topic`** clusters; fix upstream publishers or schema drift.
- Use **`make replay-dlq`** with **`DRY_RUN=1`** first; republish only after correcting root cause.
- Mark noise rows **`ignored`** or **`resolved`** in **`analytics.parse_errors`** once verified (avoid replay loops).

## Escalate

If counts rise during an otherwise stable release, suspect incompatible envelope changes—coordinate a backward-compatible producer rollout.
