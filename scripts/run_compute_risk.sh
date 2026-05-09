#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE="${COMPOSE:-docker compose}"

SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.7.3"

SPARK_SUBMIT_ENV=(
  -e PYTHONPATH=/app/src
  -e PSYCOPG_CONNINFO="host=postgres port=5432 dbname=${POSTGRES_DB:-smoke} user=${POSTGRES_USER:-smoke} password=${POSTGRES_PASSWORD:-smoke}"
  -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092
  -e JDBC_URL="jdbc:postgresql://postgres:5432/${POSTGRES_DB:-smoke}"
  -e JDBC_USER="${POSTGRES_USER:-smoke}"
  -e JDBC_PASSWORD="${POSTGRES_PASSWORD:-smoke}"
)

echo "Spark: compute smoke risk..."
${COMPOSE} exec -T \
  "${SPARK_SUBMIT_ENV[@]}" \
  spark-master /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --packages "${SPARK_PACKAGES}" \
  --conf spark.executorEnv.PYTHONPATH=/app/src \
  /app/src/wildfire_smoke/spark/compute_smoke_risk.py

echo "Risk computation complete."
