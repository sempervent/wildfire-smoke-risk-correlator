from __future__ import annotations

import json
import logging
import os
from typing import Iterable

from pyspark.sql import SparkSession

from wildfire_smoke.db.connection import connect
from wildfire_smoke.db.spatial import associate_air_quality_points
from wildfire_smoke.settings import kafka_topics


def _foreach_partition(partition: Iterable) -> None:
    import psycopg
    from kafka import KafkaProducer

    topics = kafka_topics()
    conninfo = os.environ["PSYCOPG_CONNINFO"]
    kafka_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    conn = psycopg.connect(conninfo)
    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in kafka_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    insert_raw = """
        INSERT INTO raw.openaq_measurements (source, payload)
        VALUES (%s, %s::jsonb)
        """

    upsert_norm = """
        INSERT INTO normalized.air_quality_measurements (
          measurement_id, provider, location_id, sensor_id, parameter,
          value, unit, measured_at, latitude, longitude
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s::timestamptz, %s, %s
        )
        ON CONFLICT (measurement_id) DO UPDATE SET
          provider = EXCLUDED.provider,
          location_id = EXCLUDED.location_id,
          sensor_id = EXCLUDED.sensor_id,
          parameter = EXCLUDED.parameter,
          value = EXCLUDED.value,
          unit = EXCLUDED.unit,
          measured_at = EXCLUDED.measured_at,
          latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude;
        """

    processed = 0
    try:
        with conn.cursor() as cur:
            for row in partition:
                raw_val = row.value
                if raw_val is None:
                    continue

                envelope = json.loads(raw_val.decode("utf-8"))
                record = envelope["record"]
                normalized = record["normalized"]

                cur.execute(insert_raw, ("openaq", json.dumps(envelope)))
                cur.execute(
                    upsert_norm,
                    (
                        normalized["measurement_id"],
                        normalized.get("provider"),
                        normalized.get("location_id"),
                        normalized.get("sensor_id"),
                        normalized["parameter"],
                        float(normalized["value"]),
                        normalized["unit"],
                        normalized["measured_at"],
                        float(normalized["latitude"]),
                        float(normalized["longitude"]),
                    ),
                )

                producer.send(topics["air_quality_normalized_topic"], value=normalized)
                processed += 1

        conn.commit()
        producer.flush()
    finally:
        producer.close()
        conn.close()


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    topics = kafka_topics()
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    spark = SparkSession.builder.appName("normalize-openaq").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topics["openaq_raw_topic"])
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    df.foreachPartition(_foreach_partition)

    with connect() as conn:
        associate_air_quality_points(conn)
        conn.commit()

    spark.stop()


if __name__ == "__main__":
    main()
