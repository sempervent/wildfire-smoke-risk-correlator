# Runbook: no recent wind data (`no_recent_wind_data`)

## Meaning

There are **zero** normalized wind rows whose **`observed_at`** falls inside the alert **lookback** window—distinct from staleness (which still requires at least one historical row).

## Confirm

```sql
SELECT COUNT(*) AS rows_in_lookback
FROM normalized.wind_observations w
WHERE w.observed_at >= now() - interval '24 hours';
```

## Mitigate

- Run **`make replay-wind-fixtures`** for a no-secrets smoke test.
- Confirm Kafka topics **`weather.wind.raw`** / **`weather.wind.normalized`** exist (**`make topics`**).
- Run **`make normalize-wind`** (or **`make normalize`**) after publishing raw wind messages.

## Escalate

If ingestion runs succeed but normalization yields zero rows, inspect Spark executor logs for **`normalize_wind`** failures.
