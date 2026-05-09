COMPOSE ?= docker compose

.PHONY: up down reset db-bootstrap topics ingest-once ingest-live-once normalize normalize-wind compute-plume smoke-transport-demo smoke-test test deps quality-check replay-fixtures replay-wind-fixtures grafana-up refresh-mviews alerts-check alerts-materialize alerts-send alerts-send-digest alerts-send-retry operational-cycle operational-scheduler-up demo

deps:
	uv sync --extra dev

up:
	$(COMPOSE) up -d --build postgres redpanda redpanda-console spark-master spark-worker

down:
	$(COMPOSE) down

reset:
	bash scripts/reset_local.sh

db-bootstrap:
	bash scripts/download_census_boundaries.sh
	bash scripts/load_census_boundaries.sh
	bash scripts/bootstrap_db.sh

topics:
	bash scripts/create_kafka_topics.sh

ingest-once:
	bash scripts/ingest_once.sh

ingest-live-once:
	bash scripts/live_ingest_once.sh

normalize:
	bash scripts/run_normalize.sh

normalize-wind:
	bash scripts/run_normalize_wind.sh

compute-plume:
	bash scripts/run_compute_plume.sh

compute-risk:
	bash scripts/run_compute_risk.sh

smoke-transport-demo:
	bash scripts/smoke_transport_demo.sh

quality-check:
	bash scripts/quality_check.sh

replay-fixtures:
	bash scripts/replay_fixtures.sh

replay-wind-fixtures:
	bash scripts/replay_wind_fixtures.sh

grafana-up:
	$(COMPOSE) --profile grafana up -d grafana

refresh-mviews:
	bash scripts/refresh_materialized_views.sh

alerts-check:
	bash scripts/check_alerts.sh

alerts-materialize:
	bash scripts/materialize_alerts.sh

alerts-send:
	bash scripts/send_alerts.sh

alerts-send-digest:
	ALERT_DIGEST=1 bash scripts/send_alerts.sh --digest

alerts-send-retry:
	ALERT_RETRY_QUEUE=1 bash scripts/send_alerts.sh --retry-queue

operational-cycle:
	bash scripts/run_operational_cycle.sh

operational-scheduler-up:
	$(COMPOSE) --profile scheduler up -d operational-scheduler

demo:
	bash scripts/demo_local.sh

smoke-test:
	bash scripts/smoke_test.sh

test:
	uv run pytest -q
