# Alerting and notifications

## SQL-first candidates

**`analytics.fn_alert_candidates(...)`** unions threshold-driven rows (freshness, lag, DLQ proxies, grid weather, dispersion/calibration hints). **`analytics.v_alert_candidates`** applies default numeric thresholds.

Tune behavior with **`ALERT_*`** env vars (see **[Environment](../reference/environment.md)**).

## Materialize incidents

```bash
make alerts-materialize
```

Upserts **`analytics.alert_events`** with fingerprint dedupe while incidents are open.

## Send notifications

```bash
make alerts-send
```

Supports console, webhook, Slack, SMTP — configure per **`.env.example`**. Never log raw webhook URLs or SMTP passwords.

## Runbooks

**`config/runbooks.yaml`** maps **`alert_type`** values to **`docs/runbooks/*.md`**. Adding a new candidate type in SQL requires a matching runbook entry and markdown file.

## Digest and retry

- **`make alerts-send-digest`**
- **`make alerts-send-retry`**

See **`scripts/send_alerts.sh`** and notifier modules under **`src/wildfire_smoke/notifiers/`**.
