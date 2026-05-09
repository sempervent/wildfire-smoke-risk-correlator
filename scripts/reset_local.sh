#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"

echo "Stopping stack and removing volumes..."
${COMPOSE} down -v

echo "Starting stack..."
${COMPOSE} up -d --build postgres redpanda redpanda-console spark-master spark-worker

echo "Waiting for Postgres health..."
until ${COMPOSE} exec -T postgres pg_isready -U "${POSTGRES_USER:-smoke}" -d "${POSTGRES_DB:-smoke}"; do
  sleep 1
done

echo "Waiting for Redpanda health..."
for _ in $(seq 1 60); do
  if ${COMPOSE} exec -T redpanda rpk cluster info --brokers 127.0.0.1:9092 >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

bash "${ROOT_DIR}/scripts/create_kafka_topics.sh"
bash "${ROOT_DIR}/scripts/download_census_boundaries.sh"
bash "${ROOT_DIR}/scripts/load_census_boundaries.sh"
bash "${ROOT_DIR}/scripts/bootstrap_db.sh"

echo "Reset complete."
