# Agent notes

This repository is a **local-first vertical slice** for correlating NASA FIRMS hotspots and OpenAQ particulate measurements into PostGIS census geographies, with Kafka as the ingestion buffer and Spark batch jobs for normalization. Smoke-risk scoring runs as a **Python psycopg job** inside the Spark container (`scripts/run_compute_risk.sh`).

## Project invariants

- **Never commit secrets.** FIRMS uses `FIRMS_MAP_KEY`; OpenAQ may require `OPENAQ_API_KEY` depending on access behavior.
- **Dry-run vs live:** `FIRMS_DRY_RUN=1` / `OPENAQ_DRY_RUN=1` force fixture publishing only—no NASA/OpenAQ network calls. Live ingestion requires keys and fails fast when prerequisites are missing.
- **No-secret logging:** ingestion run **`config` JSONB** must never contain API keys or map keys; producers only record safe operational fields (bbox, sources, fixture paths, YAML limits).
- **Additive migrations:** ship DDL under `sql/migrations/*.sql`; `scripts/bootstrap_db.sh` applies them before views. Keep `docker/postgres/initdb/` in sync for fresh volumes.
- **Risk disclaimer:** scores are an **engineering correlation index**, not a public-health advisory—for **v1**, **v2**, **v3**, and **v4**.
- **Meteorological wind convention:** treat **`wind_direction_degrees` as wind FROM** (origin bearing). Modeled smoke transport uses the **downwind / opposite bearing**—preserve this invariant in math and docs (`src/wildfire_smoke/wind.py`).
- **Plume model honesty:** `wind_v1` / `wind_grid_v2` / `analytics.smoke_plume_exposures` are **simple corridor heuristics**, not atmospheric dispersion, CFD, or epidemiology—never imply clinical/air-quality advisory accuracy from these scores alone. **`wind_grid_v2`** does **not** make the model dispersion-grade.
- **Wind fixtures stay no-secrets:** `WIND_DRY_RUN=1` + checked-in JSONL must remain runnable without API keys; live wind pulls stay **bounded** (`WIND_STATION_IDS` **or** `WIND_BBOX` discovery with **`WIND_STATION_DISCOVERY_LIMIT`**, optional radius buffer—never “fetch all CONUS stations” defaults).
- **Network notifier tests:** mock HTTP/SMTP transports—do not rely on live external calls in unit tests.
- **Dashboard GeoJSON views are presentation-only.** Canonical geometries live in **`geo.counties` / `geo.tracts`** (and point rows in **`normalized.*`**). Do not treat `analytics.v_latest_*_geojson` as authoritative storage.
- **No local default should pull all US tracts.** Multi-state tract downloads are explicit (`CENSUS_STATEFPS`, yaml `states:`); national tract imports are out of scope for the default workflow.
- **Alerts are SQL-first.** Ship inspectable views/functions (`analytics.fn_alert_candidates`, `v_sli_*`) before wiring external notification systems.
- **Fixture demo path stays no-secrets:** `make demo` / `make replay-fixtures` must never require `FIRMS_MAP_KEY`, `OPENAQ_API_KEY`, or wind secrets (`WIND_DRY_RUN=1` fixture path).
- **Phase 4 alerting:** `analytics.alert_events` is a **materialized incident queue** with fingerprint dedupe while `open|acknowledged`; canonical evaluation remains SQL (`fn_alert_candidates`). Notifiers must **never log secrets** (SMTP passwords, webhook URLs with tokens, API keys).
- **Phase 5 reliability:** `analytics.notification_attempts` is the **audit trail**; store **`destination_hash`** + safe error text only—never raw webhook URLs, tokens, or SMTP secrets. All notifier failures must be safe to aggregate in logs/Grafana.
- **Digest mode:** digests summarize batches but **must not silently hide criticals**—always point operators back to SQL surfaces (`fn_alert_candidates`, `v_open_alert_events`).
- **Scheduler profile:** Compose `scheduler` is **off by default**; mounting Docker sockets is sensitive—prefer **`deploy/systemd/`** timers on a secured host when possible.
- **Live ingestion stays bounded by default:** `make ingest-live-once` / `LIVE_INGEST_BBOX` enforce modest spans unless operators set **`LIVE_INGEST_ALLOW_LARGE_BBOX=1`** explicitly.
- **New alert types require runbooks:** add `docs/runbooks/*.md` **and** extend `config/runbooks.yaml` when introducing a new `alert_type` from SQL.
- **Phase 7 normalization failures:** **bad Kafka messages must not poison a batch**—quarantine per row into **`analytics.parse_errors`**, publish sanitized envelopes to **source DLQs** + **`normalization.errors`**, and keep writing valid rows. Never insert normalized rows with null required fields just to “move on.”
- **DLQ replay safety:** operator tooling defaults to **`DRY_RUN=1`** (`scripts/replay_dlq.sh`). Treat **`DLQ_RESOLVE_ON_REPLAY=1`** as explicit acknowledgement when republishing fixed payloads.
- **No secrets in failure artifacts:** **`payload_sample`**, DLQ **`original_payload`**, and logs must stay free of API keys/tokens—use `wildfire_smoke.dlq.sanitize_payload_sample` patterns.
- **Parser observability:** classify failures (`error_class`) consistently so **`analytics.parse_errors`** and alerts remain aggregate-friendly.
- **Offset concepts:** **`analytics.kafka_consumer_offsets`** is **application evidence** from Spark batch jobs. **`analytics.kafka_topic_offsets`** / **`analytics.kafka_consumer_lag_observations`** store **broker watermark snapshots** and **application-observed lag** (high minus recorded offset)—do not conflate these with broker consumer-group commit lag unless tooling explicitly aligns them.
- **Lag collection resilience:** `collect_kafka_lag` / operational **`collect_lag`** steps must **not** fail scheduled cycles by default; operators opt into hard failure via **`STRICT_LAG_COLLECTION=1`**.
- **Replay bookkeeping:** `replay-dlq` defaults to **`DRY_RUN=1`**; **`DLQ_REPLAY_BOOKKEEPING`** defaults on — preserve audit rows instead of deleting parse-error history.
- **Parse errors lifecycle:** **`parse-errors-compact`** defaults to **report-only** (`DRY_RUN=1`); avoid deleting **`parse_errors`** rows — archival to **`archived`** is explicit opt-in.
- **Phase 9 gridded weather:** live ingest stays **bounded** (`GRID_WEATHER_BBOX` span guards mirror **`LIVE_INGEST_*`**; **`GRID_WEATHER_REFUSE_LARGE_BBOX`** defaults on). **`GRID_WEATHER_DRY_RUN=1`** + checked-in fixtures must remain **no-secrets**. Malformed grid payloads belong in **`analytics.parse_errors`** and **`weather.grid.dlq`** like other normalizers—never silently drop accountability.
- **Phase 10 integration regression:** **`make integration-regression`** must stay **no-secrets** (fixture producers + Spark + Postgres only). Prefer **`SKIP_BOOTSTRAP=1`** / **`RUN_BOOTSTRAP=0`** in CI-style runs unless census download is explicitly intended.
- **Aligned fixtures:** **`USE_ALIGNED_FIXTURES=1`** samples under **`tests/fixtures/*_aligned_sample.*`** should stay **deterministic** (fixed coordinates and relative ordering) so integration assertions remain stable across machines.
- **Live NWS gridpoint ingest:** keep **`GRID_WEATHER_BBOX`** / **`GRID_WEATHER_POINTS`** + **`GRID_WEATHER_MAX_POINTS`** as the primary bounds—never expand to “whole CONUS” by default.
- **Fixture timestamp rewriting:** **`FIXTURE_TIME_MODE=relative`** mutates **Kafka payloads in memory only**; **never rewrite fixture files on disk** as part of normal replay.
- **Calibration hooks:** **`analytics.risk_observations`**, **`analytics.risk_model_evaluations`**, and **`make evaluate-risk`** are **evaluation scaffolding**, not validated epidemiology or forecasting science—do not treat outputs as peer-reviewed metrics without external methodology.
- **Phase 11 Gaussian proxy:** never describe **`gaussian_v0`** as validated atmospheric dispersion, HYSPLIT-class transport, or regulatory modeling—it is a **bounded engineering correlation weight** over census centroids. **`wind_v1` / `wind_grid_v2`** remain separate corridor heuristics; all three are **not** public-health advisories.
- **Dispersion runs stay bounded:** respect **`DISPERSION_MAX_DISTANCE_KM`**, **`DISPERSION_MAX_TARGET_GEOGRAPHIES`**, **`DISPERSION_LOOKBACK_HOURS`**, and tract corpus guards (**`DISPERSION_ALLOW_LARGE_RUN`**)—no implicit national-scale tract fan-out.
- **AQ comparison table:** **`analytics.dispersion_aq_comparisons`** and **`make compare-dispersion-aq`** are **lag-summary scaffolding** only; they do not establish forecast skill or epidemiological association without external study design.
- **Fixture / integration path:** `make integration-regression` and **`make dispersion-demo`** must remain **no live API keys** when using dry-run + aligned fixtures.
- **Phase 12 calibration honesty:** **`evidence_label`**, **`confidence_label`**, and Grafana calibration panels are **engineering triage aids** — never describe them as validated model performance, epidemiology, or regulatory proof.
- **No-data ≠ success:** absence of AQ rows or evaluations must **not** be marketed as the model “passing”; distinguish **no data**, **insufficient data**, and **weak evidence** in prose and dashboards.
- **Correlation gate:** do **not** treat Pearson **`correlation`** in **`risk_model_evaluations`** as meaningful when **`match_count`** is below **`RISK_EVAL_MIN_MATCH_COUNT`** (default **3**) or when variance collapses — the job omits **`correlation`** in those cases.
- **Calibration alerts stay soft:** default **`ALERT_CALIBRATION_WARN_ONLY=1`** maps SQL severities toward **`info`** — do not escalate calibration mismatch alerts to paging without explicit ops configuration.
- **Fixture observations:** JSONL under **`tests/fixtures/`** used by **`make load-risk-observation-fixtures`** must stay **deterministic and secret-free** (proxy labels only).
- **Phase 13 CI hygiene:** default **GitHub Actions PR CI** must stay **no secrets**, **no live vendor APIs**, and **must not download Census** — use **`SMOKE_NO_COMPOSE=1`** / unit tests instead of full **`make smoke-test`** there.
- **Census in CI:** never rely on **TIGER downloads** in PR CI; use **`make db-bootstrap-minimal`** + synthetic **`tests/fixtures/census_minimal_*.geojson`** only for optional integration workflows.
- **Release artifacts honesty:** **`CHANGELOG.md`**, **`docs/release/*.md`**, and tagged milestones must **not** claim peer-reviewed scientific validation — describe **engineering scope** and **limitations** explicitly.
- **Calibration exports:** **`artifacts/calibration/*`** bundles are **immutable review snapshots** for operators — not regulatory submissions; **`metadata.json`** must remain free of **passwords**, raw **DSN strings**, **API keys**, and **webhook secrets** (hosts are redacted away from localhost).
- **Integration workflows:** Compose-backed jobs may stay **manual**, **scheduled**, and/or **label-gated** when too heavy for every PR — prefer fast static CI on pushes.

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
- **Delivery reliability + ops instrumentation (Phase 5)**: `sql/migrations/004_phase5_notification_reliability.sql`, `docker/postgres/initdb/60_phase5.sql`, `src/wildfire_smoke/alert_delivery.py`, `src/wildfire_smoke/notification_reliability.py`, `src/wildfire_smoke/digest.py`, `src/wildfire_smoke/operational_runs.py`, `docker/scheduler/loop.sh`, `deploy/systemd/`

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
# Phase 7 deep validation (Spark normalizers + poison messages): make dlq-smoke-test
make refresh-mviews
make grafana-up
```

Optional: `make demo` for a guided no-secrets loop (starts services and replays fixtures).

Fixture note: `make alerts-check` without `ALERTS_WARN_ONLY` commonly exits non-zero because fixture timestamps look “stale” vs freshness thresholds — this is expected.
