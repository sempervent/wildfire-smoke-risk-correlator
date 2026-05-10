# Troubleshooting

## Duplicate `fn_alert_candidates` overloads

Error: **`function analytics.fn_alert_candidates(...) is not unique`**.

**Fix:** **`make repair-alert-function`** then **`make db-doctor`**. Legacy volumes only — fresh **`bootstrap_db`** order should prevent this.

## Missing views / migrations

Symptoms: **`relation ... does not exist`** when querying **`analytics.v_*`**.

**Fix:** Run **`make db-bootstrap`** or **`make db-bootstrap-minimal`** on the target database, then verify **`make db-doctor`**.

## Stale fixture timestamps vs alerts

**`make alerts-check`** may report critical staleness for checked-in fixtures. Use **`ALERTS_WARN_ONLY=1`** for demos or widen freshness thresholds.

## Spark / JDBC

Spark normalizers need **`PSYCOPG_CONNINFO`** (or JDBC URL + credentials) and **`KAFKA_BOOTSTRAP_SERVERS`** inside executor containers — see **`scripts/run_normalize.sh`**.

## Grafana port conflicts

Set **`GRAFANA_PORT`** to a free host port if **`make grafana-up`** fails to bind.

## Compose issues

Ensure **`docker compose`** matches **`docker-compose.yml`** services; Postgres healthcheck must pass before **`make smoke-test`**.

## GDAL / Census paths

**`gdal-utils`** profile mounts **`./data/raw/census`** at **`/data/census`** for loaders.

See also **[Database doctor](db-doctor.md)** and **`docs/release/v1.0.1.md`** (overload repair sequence).
