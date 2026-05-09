# Stale OpenAQ-derived measurements (`stale_openaq_normalized`)

## Meaning

`MAX(normalized.air_quality_measurements.measured_at)` is older than freshness thresholds.

## Likely causes

- OpenAQ producer not running or blocked (HTTP errors, auth).
- Normalization lag or failures.
- Fixture/demo JSONL with **old** measurement times.

## First checks

```bash
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT MAX(measured_at) FROM normalized.air_quality_measurements;"
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT * FROM analytics.ingestion_runs WHERE source='openaq' ORDER BY started_at DESC LIMIT 5;"
```

## Remediation

- Confirm `OPENAQ_API_KEY` if your tenant requires it.
- Re-run bounded live ingest (`make ingest-live-once`) or fixture replay for offline validation.

## Fixture / demo mode

Expect staleness warnings against “now”; use warn-only alert checks when validating plumbing.
