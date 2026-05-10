# No recent normalized fire rows (`no_recent_fire_detections`)

## Meaning

Within the configured **lookback window**, there are **zero** `normalized.fire_detections` rows (or newest rows are themselves stale vs critical thresholds).

## Likely causes

- No hotspots in bbox for the window (possible legitimately).
- Ingestion/normalization stopped.
- Overly tight geography vs where FIRMS published activity.

## First checks

```bash
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT COUNT(*) FILTER (WHERE acq_datetime >= now() - interval '24 hours') AS n24 FROM normalized.fire_detections;"
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT * FROM analytics.v_sli_no_recent_data WHERE dataset='fire_detections';"
```

## Remediation

- Confirm FIRMS producer configuration (`FIRMS_BBOX`, `FIRMS_DAY_RANGE`).
- Run Spark normalization; verify Kafka topic lag.

## Fixture / demo mode

Small fixtures may yield zero rows in lookback vs wall clock—use historical replay understanding or widen `ALERT_LOOKBACK_HOURS` **only** for local experiments.
