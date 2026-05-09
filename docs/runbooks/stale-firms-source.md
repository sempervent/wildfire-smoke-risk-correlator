# Stale FIRMS-derived fire timestamps (`stale_firms_normalized`)

## Meaning

`MAX(normalized.fire_detections.acq_datetime)` is older than freshness thresholds (warn/critical hours). The pipeline is not seeing **recent** hotspot activity in normalized storage.

## Likely causes

- Live FIRMS ingest not running or failing silently upstream.
- Kafka/Spark normalization backlog or job failures.
- Fixture/demo data with **old** timestamps (expected locally).

## First checks

```bash
make alerts-check ALERTS_WARN_ONLY=1
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT * FROM analytics.v_sli_source_freshness WHERE metric = 'fire_detections_max_acq';"
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT MAX(acq_datetime) FROM normalized.fire_detections;"
```

## Remediation

- Confirm producers succeed (`analytics.ingestion_runs` for `source=firms`).
- Run `make normalize` after ingestion; inspect Spark logs.
- For bounded live tests, run `make ingest-live-once` with a **small** bbox.

## Fixture / demo mode

Checked-in fixtures use historical timestamps—**ignore** this alert when validating mechanics; use `ALERTS_WARN_ONLY=1` for `make alerts-check` and expect candidates in `v_alert_candidates`.
