# Runbook: stale wind source (`wind_data_stale`)

## Meaning

The newest **`normalized.wind_observations.observed_at`** is older than freshness thresholds (same warn/critical hours pattern as FIRMS/OpenAQ staleness alerts).

## Confirm

```sql
SELECT MAX(observed_at) AS newest_wind_observed_at
FROM normalized.wind_observations;
```

Check recent **`analytics.ingestion_runs`** rows where **`source = 'wind'`**.

## Mitigate

- Fixture path: run **`make replay-wind-fixtures`** or **`WIND_DRY_RUN=1`** producers + **`make normalize-wind`**.
- Live path: ensure **`WIND_STATION_IDS`** is set (bounding-box discovery is not implemented in v1) and **`WIND_DRY_RUN=0`** for **`scripts/ingest_once.sh`** / live ingest.

## Escalate

If NWS intermittently fails, capture HTTP errors from producer logs and widen retry/backoff in a future phase.
