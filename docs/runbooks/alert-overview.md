# Alert overview

Alerts start as **rows from `analytics.fn_alert_candidates`** (thresholds via `ALERT_*` env vars). **Materialization** turns them into **`analytics.alert_events`** so duplicates collapse per stable **fingerprint**, then optional **notifiers** emit messages.

## Lifecycle

1. **Candidate** — SQL evaluation surfaces current problems (freshness, ingestion failures, elevated risk).
2. **Materialize** — `make alerts-materialize` upserts `analytics.alert_events` (updates `last_seen_at` when the fingerprint already exists as `open`/`acknowledged`).
3. **Notify** — `make alerts-send` formats **open** rows for the configured notifier and records send metadata under `notification_state` (per notifier key).
4. **Resolve** — Automatic optional pass (`ALERTS_RESOLVE_MISSING=1`) closes **open** incidents missing from the latest candidate set; fixture/demo datasets often oscillate—see individual runbooks.

## Inspect first

```bash
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT alert_type, severity, title, observed_at FROM analytics.v_alert_candidates ORDER BY observed_at DESC LIMIT 50;"
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT fingerprint, alert_type, severity, status, last_seen_at FROM analytics.alert_events ORDER BY last_seen_at DESC LIMIT 50;"
```

## Severity mapping (notifications)

SQL emits `warn` / `critical`. Stored severities normalize to `warning`, `high` (elevated smoke risk below “critical” tier), or `critical`. Notifier filtering uses `ALERT_SEVERITY_MIN` against that normalized scale.
