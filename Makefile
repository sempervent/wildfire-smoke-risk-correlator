COMPOSE ?= docker compose

.PHONY: up down reset db-bootstrap db-bootstrap-minimal topics ingest-once ingest-live-once normalize normalize-wind normalize-grid-weather match-fire-weather compute-plume compute-dispersion compare-dispersion-aq dispersion-demo load-risk-observation-fixtures calibration-summary calibration-demo smoke-transport-demo smoke-test dlq-smoke-test grid-weather-demo grid-weather-smoke-test integration-regression integration-smoke-test assert-integration-state evaluate-risk test deps quality-check replay-fixtures replay-grid-weather-fixtures replay-wind-fixtures replay-bad-fixtures replay-dlq parse-errors parse-errors-compact consumer-offsets collect-lag kafka-lag grafana-up refresh-mviews alerts-check alerts-materialize alerts-send alerts-send-digest alerts-send-retry operational-cycle operational-scheduler-up demo export-calibration export-calibration-csv export-calibration-parquet release-check version

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

db-bootstrap-minimal:
	bash scripts/load_minimal_census_fixtures.sh
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

normalize-grid-weather:
	bash scripts/run_normalize_grid_weather.sh

match-fire-weather:
	bash scripts/run_match_fire_weather.sh

replay-grid-weather-fixtures:
	bash scripts/replay_grid_weather_fixtures.sh

grid-weather-demo:
	bash scripts/grid_weather_demo.sh

grid-weather-smoke-test:
	GRID_WEATHER_SMOKE=1 bash scripts/smoke_test.sh

integration-regression:
	bash scripts/integration_regression.sh

integration-smoke-test:
	bash scripts/integration_smoke_test.sh

assert-integration-state:
	bash scripts/assert_integration_state.sh

evaluate-risk:
	bash scripts/evaluate_risk_model.sh

load-risk-observation-fixtures:
	bash scripts/load_risk_observation_fixtures.sh

calibration-summary:
	bash scripts/calibration_summary.sh

calibration-demo:
	bash scripts/calibration_demo.sh

compute-plume:
	bash scripts/run_compute_plume.sh

compute-dispersion:
	bash scripts/run_compute_dispersion.sh

compare-dispersion-aq:
	bash scripts/run_compare_dispersion_aq.sh

dispersion-demo:
	bash scripts/dispersion_demo.sh

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

replay-bad-fixtures:
	bash scripts/replay_bad_fixtures.sh

replay-dlq:
	bash scripts/replay_dlq.sh

dlq-smoke-test:
	bash scripts/dlq_smoke_test.sh

parse-errors:
	$(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-smoke}" -d "$${POSTGRES_DB:-smoke}" -c "SELECT * FROM analytics.v_parse_error_summary;"

consumer-offsets:
	$(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-smoke}" -d "$${POSTGRES_DB:-smoke}" -c "SELECT * FROM analytics.v_consumer_offset_state;"

collect-lag:
	bash scripts/collect_kafka_lag.sh

kafka-lag: collect-lag

parse-errors-compact:
	bash scripts/compact_parse_errors.sh

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

export-calibration:
	bash scripts/export_calibration.sh

export-calibration-csv:
	CALIBRATION_EXPORT_INCLUDE_PARQUET=0 bash scripts/export_calibration.sh

export-calibration-parquet:
	CALIBRATION_EXPORT_INCLUDE_PARQUET=1 bash scripts/export_calibration.sh

release-check:
	bash scripts/release_check.sh

version:
	@uv run python -c 'from wildfire_smoke import __version__ as v; print("package_version", v)'
	@if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then \
		echo "git_commit $$(git rev-parse HEAD)"; \
		echo "git_branch $$(git rev-parse --abbrev-ref HEAD)"; \
		if [[ -n $$(git status --porcelain 2>/dev/null) ]]; then echo "working_tree dirty"; else echo "working_tree clean"; fi; \
	else \
		echo "git unavailable"; \
	fi

test:
	uv run pytest -q
