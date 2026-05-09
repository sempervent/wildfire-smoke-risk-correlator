from __future__ import annotations

import json
import logging
import os
from typing import Iterable

from pyspark.sql import SparkSession

from wildfire_smoke.db.connection import connect
from wildfire_smoke.db.spatial import associate_wind_points
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
        INSERT INTO raw.wind_observations (source, payload)
        VALUES (%s, %s::jsonb)
        """

    upsert_norm = """
        INSERT INTO normalized.wind_observations (
          wind_observation_id, source, station_id,
          observed_at, latitude, longitude,
          wind_speed_mps, wind_direction_degrees, wind_gust_mps
        ) VALUES (
          %s, %s, %s, %s::timestamptz, %s, %s, %s, %s, %s
        )
        ON CONFLICT (wind_observation_id) DO UPDATE SET
          source = EXCLUDED.source,
          station_id = EXCLUDED.station_id,
          observed_at = EXCLUDED.observed_at,
          latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude,
          wind_speed_mps = EXCLUDED.wind_speed_mps,
          wind_direction_degrees = EXCLUDED.wind_direction_degrees,
          wind_gust_mps = EXCLUDED.wind_gust_mps;
        """

    processed = 0
    try:
        with conn.cursor() as cur:
            for row in partition:
                raw_val = row.value
                if raw_val is None:
                    continue

                envelope = json.loads(raw_val.decode("utf-8"))
                src = str(envelope.get("source") or "unknown")
                normalized = envelope["record"]["normalized"]

                cur.execute(insert_raw, (src, json.dumps(envelope)))
                cur.execute(
                    upsert_norm,
                    (
                        normalized["wind_observation_id"],
                        normalized["source"],
                        normalized.get("station_id"),
                        normalized["observed_at"],
                        float(normalized["latitude"]),
                        float(normalized["longitude"]),
                        float(normalized["wind_speed_mps"]) if normalized.get("wind_speed_mps") is not None else None,
                        float(normalized["wind_direction_degrees"])
                        if normalized.get("wind_direction_degrees") is not None
                        else None,
                        float(normalized["wind_gust_mps"]) if normalized.get("wind_gust_mps") is not None else None,
                    ),
                )

                producer.send(topics["wind_normalized_topic"], value=normalized)
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

    spark = SparkSession.builder.appName("normalize-wind").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topics["wind_raw_topic"])
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    df.foreachPartition(_foreach_partition)

    with connect() as conn:
        associate_wind_points(conn)
        conn.commit()

    spark.stop()


if __name__ == "__main__":
    main()
