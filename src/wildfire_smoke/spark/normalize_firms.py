from __future__ import annotations

import json
import logging
import os
from typing import Iterable

from pyspark.sql import SparkSession

from wildfire_smoke.db.connection import connect
from wildfire_smoke.db.spatial import associate_fire_points
from wildfire_smoke.settings import kafka_topics


def _foreach_partition(partition: Iterable) -> None:
    import psycopg
    from kafka import KafkaProducer

    from wildfire_smoke.firms_csv import normalized_fire_fields

    topics = kafka_topics()
    conninfo = os.environ["PSYCOPG_CONNINFO"]
    kafka_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    conn = psycopg.connect(conninfo)
    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in kafka_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    insert_raw = """
        INSERT INTO raw.firms_hotspots (source, payload)
        VALUES (%s, %s::jsonb)
        """

    upsert_norm = """
        INSERT INTO normalized.fire_detections (
          detection_id, source, latitude, longitude, acq_datetime, confidence, brightness, frp, daynight
        ) VALUES (
          %s, %s, %s, %s, %s::timestamptz, %s, %s, %s, %s
        )
        ON CONFLICT (detection_id) DO UPDATE SET
          source = EXCLUDED.source,
          latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude,
          acq_datetime = EXCLUDED.acq_datetime,
          confidence = EXCLUDED.confidence,
          brightness = EXCLUDED.brightness,
          frp = EXCLUDED.frp,
          daynight = EXCLUDED.daynight;
        """

    processed = 0
    try:
        with conn.cursor() as cur:
            for row in partition:
                raw_val = row.value
                if raw_val is None:
                    continue

                envelope = json.loads(raw_val.decode("utf-8"))
                source = envelope["source"]
                record = envelope["record"]

                cur.execute(insert_raw, (source, json.dumps(envelope)))
                fields = normalized_fire_fields(source, record)
                cur.execute(
                    upsert_norm,
                    (
                        fields["detection_id"],
                        fields["source"],
                        fields["latitude"],
                        fields["longitude"],
                        fields["acq_datetime"].isoformat(),
                        fields["confidence"],
                        fields["brightness"],
                        fields["frp"],
                        fields["daynight"],
                    ),
                )

                norm_msg = {
                    "detection_id": fields["detection_id"],
                    "source": fields["source"],
                    "latitude": fields["latitude"],
                    "longitude": fields["longitude"],
                    "acq_datetime": fields["acq_datetime"].isoformat(),
                    "confidence": fields["confidence"],
                    "brightness": fields["brightness"],
                    "frp": fields["frp"],
                    "daynight": fields["daynight"],
                }
                producer.send(topics["fire_normalized_topic"], value=norm_msg)
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

    spark = SparkSession.builder.appName("normalize-firms").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topics["firms_raw_topic"])
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    df.foreachPartition(_foreach_partition)

    with connect() as conn:
        associate_fire_points(conn)
        conn.commit()

    spark.stop()


if __name__ == "__main__":
    main()
