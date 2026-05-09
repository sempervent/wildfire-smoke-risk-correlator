# Agent notes

This repository is a **local-first vertical slice** for correlating NASA FIRMS hotspots and OpenAQ particulate measurements into PostGIS census geographies, with Kafka as the ingestion buffer and Spark batch jobs for normalization and scoring.

## Operating constraints

- **Never commit secrets.** FIRMS uses `FIRMS_MAP_KEY`; OpenAQ may require `OPENAQ_API_KEY` depending on access behavior.
- **Do not fake external APIs** in the default (“live”) ingestion path. Use **explicit fixture dry-run** (`FIRMS_DRY_RUN=1`, `OPENAQ_DRY_RUN=1`) when keys/network are unavailable.
- **Fail loudly.** Avoid bare `except:` and avoid silently ignoring producer/consumer failures.
- Prefer **scripted bootstrap** over manual DB clicks (`scripts/*.sh`, `Makefile` targets).

## Where to change what

- **Compose topology**: `docker-compose.yml`
- **DDL**: `docker/postgres/initdb/*.sql`
- **Census bootstrap**: `scripts/download_census_boundaries.sh`, `scripts/load_census_boundaries.sh`, `config/census.yaml`
- **Kafka topics**: `scripts/create_kafka_topics.sh`
- **Producers**: `src/wildfire_smoke/producers/*.py`
- **Spark jobs**: `src/wildfire_smoke/spark/*.py` (submit wrappers in `scripts/run_*.sh`)
- **Analytical SQL**: `sql/views/*.sql`, `sql/queries/*.sql`

## Spark + Python imports

Spark executors run the custom image `docker/spark/Dockerfile` (based on `apache/spark:3.5.4-java17-python3`) with extra Python wheels: `kafka-python-ng`, `psycopg`, and `pyyaml`. Application code is mounted at `/app`, with `PYTHONPATH=/app/src`.

## Risk scoring disclaimer

The score is an **engineering index** for correlation and pipeline validation, **not** a public health advisory.
