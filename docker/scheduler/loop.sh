#!/bin/sh
set -eu

INTERVAL="${OPERATIONAL_INTERVAL_SECONDS:-3600}"

echo "[operational-scheduler] boot LIVE_MODE=${LIVE_MODE:-0} interval=${INTERVAL}s"

while true; do
  echo "[operational-scheduler] cycle start $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  docker compose -f /workspace/docker-compose.yml exec -T \
    -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
    -e POSTGRES_HOST=postgres \
    -e POSTGRES_PORT=5432 \
    -e POSTGRES_DB="${POSTGRES_DB:-smoke}" \
    -e POSTGRES_USER="${POSTGRES_USER:-smoke}" \
    -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-smoke}" \
    -e LIVE_MODE="${LIVE_MODE:-0}" \
    spark-worker bash -lc 'cd /app && export PYTHONPATH=/app/src PATH=/usr/local/bin:$PATH; bash scripts/run_operational_cycle.sh' \
    && echo "[operational-scheduler] cycle ok" \
    || echo "[operational-scheduler] cycle failed"
  sleep "$INTERVAL"
done
