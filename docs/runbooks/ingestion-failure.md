# Ingestion failure (`ingestion_failed`)

## Meaning

An `analytics.ingestion_runs` row finished as **`failed`** within the alert lookback window.

## Likely causes

- Missing/invalid API credentials for live sources.
- HTTP errors/timeouts from FIRMS or OpenAQ.
- Kafka publish failures or deserialization issues surfaced as failures.

## First checks

```bash
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT run_id, source, mode, status, error_message, started_at, finished_at FROM analytics.ingestion_runs WHERE status='failed' ORDER BY started_at DESC LIMIT 20;"
```

## Remediation

- Fix credentials/network; rerun bounded ingest.
- For fixtures, dry-run producers should not hit live APIs—confirm `FIRMS_DRY_RUN` / `OPENAQ_DRY_RUN`.

## Fixture / demo mode

Failures should be rare in dry-run; if seen, inspect `error_message` (never contains secrets—keys are redacted in logs).
