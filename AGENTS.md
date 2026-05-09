# Agent notes

This repository is a **local-first vertical slice** for correlating NASA FIRMS hotspots and OpenAQ particulate measurements into PostGIS census geographies, with Kafka as the ingestion buffer and Spark batch jobs for normalization. Smoke-risk scoring runs as a **Python psycopg job** inside the Spark container (`scripts/run_compute_risk.sh`).

## Project invariants

- **Never commit secrets.** FIRMS uses `FIRMS_MAP_KEY`; OpenAQ may require `OPENAQ_API_KEY` depending on access behavior.
- **Dry-run vs live:** `FIRMS_DRY_RUN=1` / `OPENAQ_DRY_RUN=1` force fixture publishing only—no NASA/OpenAQ network calls. Live ingestion requires keys and fails fast when prerequisites are missing.
- **No-secret logging:** ingestion run **`config` JSONB** must never contain API keys or map keys; producers only record safe operational fields (bbox, sources, fixture paths, YAML limits).
- **Additive migrations:** ship DDL under `sql/migrations/*.sql`; `scripts/bootstrap_db.sh` applies them before views. Keep `docker/postgres/initdb/` in sync for fresh volumes.
- **Risk disclaimer:** scores are an **engineering correlation index**, not a public-health advisory—for both **v1** and **v2**.

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
```

Optional: `make grafana-up` and confirm provisioning paths inside the container (`/etc/grafana/provisioning`, `/var/lib/grafana/dashboards`).
