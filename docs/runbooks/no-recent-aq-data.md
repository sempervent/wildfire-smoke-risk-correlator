# No recent normalized AQ measurements (`no_recent_air_quality`)

## Meaning

Within the lookback window, **no** `normalized.air_quality_measurements` rows qualify—OpenAQ coverage gaps or pipeline stalls.

## Likely causes

- Sparse sensors in bbox + short window.
- Producer/auth/network errors.
- Normalization failures.

## First checks

```bash
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT COUNT(*) FILTER (WHERE measured_at >= now() - interval '24 hours') AS n24 FROM normalized.air_quality_measurements;"
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT * FROM analytics.v_sli_no_recent_data WHERE dataset='air_quality_measurements';"
```

## Remediation

- Validate OpenAQ bbox overlap with active locations.
- Inspect OpenAQ producer logs and `analytics.ingestion_runs`.

## Fixture / demo mode

Fixture timelines often disagree with “now”; treat as operational noise unless validating live freshness.
