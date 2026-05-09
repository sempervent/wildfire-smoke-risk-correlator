# Agent notes

This repository is a **local-first vertical slice** for correlating NASA FIRMS hotspots and OpenAQ particulate measurements into PostGIS census geographies, with Kafka as the ingestion buffer and Spark batch jobs for normalization. Smoke-risk scoring runs as a **Python psycopg job** inside the Spark container (`scripts/run_compute_risk.sh`).

## Project invariants

- **Never commit secrets.** FIRMS uses `FIRMS_MAP_KEY`; OpenAQ may require `OPENAQ_API_KEY` depending on access behavior.
- **Dry-run vs live:** `FIRMS_DRY_RUN=1` / `OPENAQ_DRY_RUN=1` force fixture publishing only—no NASA/OpenAQ network calls. Live ingestion requires keys and fails fast when prerequisites are missing.
- **No-secret logging:** ingestion run **`config` JSONB** must never contain API keys or map keys; producers only record safe operational fields (bbox, sources, fixture paths, YAML limits).
- **Additive migrations:** ship DDL under `sql/migrations/*.sql`; `scripts/bootstrap_db.sh` applies them before views. Keep `docker/postgres/initdb/` in sync for fresh volumes.
- **Risk disclaimer:** scores are an **engineering correlation index**, not a public-health advisory—for both **v1** and **v2**.
- **Dashboard GeoJSON views are presentation-only.** Canonical geometries live in **`geo.counties` / `geo.tracts`** (and point rows in **`normalized.*`**). Do not treat `analytics.v_latest_*_geojson` as authoritative storage.
- **No local default should pull all US tracts.** Multi-state tract downloads are explicit (`CENSUS_STATEFPS`, yaml `states:`); national tract imports are out of scope for the default workflow.
- **Alerts are SQL-first.** Ship inspectable views/functions (`analytics.fn_alert_candidates`, `v_sli_*`) before wiring external notification systems.
- **Fixture demo path stays no-secrets:** `make demo` / `make replay-fixtures` must never require `FIRMS_MAP_KEY` or `OPENAQ_API_KEY`.
- **Phase 4 alerting:** `analytics.alert_events` is a **materialized incident queue** with fingerprint dedupe while `open|acknowledged`; canonical evaluation remains SQL (`fn_alert_candidates`). Notifiers must **never log secrets** (SMTP passwords, webhook URLs with tokens, API keys).
- **Live ingestion stays bounded by default:** `make ingest-live-once` / `LIVE_INGEST_BBOX` enforce modest spans unless operators set **`LIVE_INGEST_ALLOW_LARGE_BBOX=1`** explicitly.
- **New alert types require runbooks:** add `docs/runbooks/*.md` **and** extend `config/runbooks.yaml` when introducing a new `alert_type` from SQL.

## Operating constraints

- **Do not fake external APIs** in the default (“live”) ingestion path. Use **explicit fixture dry-run** when keys/network are unavailable.
- **Fail loudly.** Avoid bare `except:` and avoid silently ignoring producer/consumer failures.
- Prefer **scripted bootstrap** over manual DB clicks (`scripts/*.sh`, `Makefile` targets).

## Where to change what

- **Compose topology**: `docker-compose.yml` (core stack + optional **`grafana` profile**)
- **DDL / migrations**: `docker/postgres/initdb/*.sql`, `sql/migrations/*.sql`
- **Census bootstrap**: `scripts/download_census_boundaries.sh`, `scripts/load_census_boundaries.sh`, `config/census.yaml`
- **Kafka topics**: `scripts/create_kafka_topics.sh`
- **Producers**: `src/wildfire_smoke/producers/*.py`, `src/wildfire_smoke/ingestion_runs.py`
- **Spark jobs**: `src/wildfire_smoke/spark/*.py` (submit wrappers in `scripts/run_*.sh`)
- **Risk settings**: `SMOKE_RISK_*` env vars (`Settings` in `src/wildfire_smoke/settings.py`)
- **Analytical SQL**: `sql/views/*.sql`, `sql/queries/*.sql`
- **Quality / replay**: `scripts/quality_check.sh`, `scripts/replay_fixtures.sh`
- **Grafana**: `docker/grafana/provisioning/`, `docker/grafana/dashboards/`
- **Maps / SLIs (Phase 3)**: `sql/views/zzz_phase3_*.sql`, `scripts/check_alerts.sh`, `scripts/refresh_materialized_views.sh`, `scripts/demo_local.sh`, `src/wildfire_smoke/census_config.py`, `src/wildfire_smoke/alert_thresholds.py`
- **Alert persistence / notifications (Phase 4)**: `sql/migrations/003_phase4_alerts.sql`, `docker/postgres/initdb/50_phase4.sql`, `src/wildfire_smoke/alerts.py`, `src/wildfire_smoke/notifiers/`, `src/wildfire_smoke/severity.py`, `src/wildfire_smoke/live_bbox.py`, `scripts/materialize_alerts.sh`, `scripts/send_alerts.sh`, `scripts/live_ingest_once.sh`, `scripts/run_operational_cycle.sh`, `config/runbooks.yaml`, `docs/runbooks/`

## Spark + Python imports

Spark executors run the custom image `docker/spark/Dockerfile` (based on `apache/spark:3.5.4-java17-python3`) with extra Python wheels: `kafka-python-ng`, `psycopg`, `pyyaml`, `python-dotenv`. Application code is mounted at `/app`, with `PYTHONPATH=/app/src`. Normalization still uses **`spark-submit`** with Ivy-fetched Kafka/SQL packages; **`compute_smoke_risk`** uses **`python3`** only (no Spark session).

## Validation before commit

From a clean stack (or existing volume with migrations applied), maintainers should verify:

```bash
make deps && make test
make up && make topics && make db-bootstrap
make replay-fixtures
make normalize && make compute-risk
make quality-check && make smoke-test
make alerts-check ALERTS_WARN_ONLY=1
make refresh-mviews
make grafana-up
```

Optional: `make demo` for a guided no-secrets loop (starts services and replays fixtures).

Fixture note: `make alerts-check` without `ALERTS_WARN_ONLY` commonly exits non-zero because fixture timestamps look “stale” vs freshness thresholds — this is expected.
