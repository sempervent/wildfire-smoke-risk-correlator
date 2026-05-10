#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"

TOPICS=(
  "firms.hotspots.raw"
  "firms.hotspots.dlq"
  "openaq.measurements.raw"
  "openaq.measurements.dlq"
  "weather.wind.raw"
  "weather.wind.dlq"
  "weather.wind.normalized"
  "weather.grid.raw"
  "weather.grid.dlq"
  "weather.grid.normalized"
  "normalization.errors"
  "fire.detections.normalized"
  "air_quality.measurements.normalized"
  "smoke.risk.scores"
  "deadletter.events"
)

for t in "${TOPICS[@]}"; do
  echo "Ensuring topic exists: ${t}"
  if ${COMPOSE} exec -T redpanda rpk topic describe "${t}" --brokers 127.0.0.1:9092 >/dev/null 2>&1; then
    echo "  already exists"
    continue
  fi
  ${COMPOSE} exec -T redpanda rpk topic create "${t}" --brokers 127.0.0.1:9092 -p 3 -r 1
  echo "  created"
done

echo "Topics:"
${COMPOSE} exec -T redpanda rpk topic list --brokers 127.0.0.1:9092
