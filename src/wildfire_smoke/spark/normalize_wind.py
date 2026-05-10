from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from pyspark.sql import SparkSession

from wildfire_smoke.db.connection import connect
from wildfire_smoke.db.spatial import associate_wind_points
from wildfire_smoke.settings import kafka_topics


def _foreach_partition(partition: Iterable) -> None:
    import psycopg
    from kafka import KafkaProducer

    from wildfire_smoke.dlq import handle_normalize_failure, upsert_consumer_offset

    log_l = logging.getLogger(__name__)
    topics = kafka_topics()
    conninfo = os.environ["PSYCOPG_CONNINFO"]
    kafka_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    consumer_group = os.environ.get("NORMALIZER_CONSUMER_GROUP", "spark-normalize-wind")
    source_topic = topics["wind_raw_topic"]
    target_dataset = "normalized.wind_observations"

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

    partition_state: dict[tuple[str, int], dict[str, Any]] = {}

    def _meta(row: Any) -> tuple[str, int, int, str | None]:
        t = row.topic if hasattr(row, "topic") else row["topic"]
        p = int(row.partition if hasattr(row, "partition") else row["partition"])
        o = int(row.offset if hasattr(row, "offset") else row["offset"])
        raw_k = row.key if hasattr(row, "key") else row["key"]
        if raw_k is None:
            mk = None
        elif isinstance(raw_k, (bytes, bytearray)):
            mk = raw_k.decode("utf-8", errors="replace")
        else:
            mk = str(raw_k)
        return str(t), p, o, mk

    try:
        with conn.cursor() as cur:
            for row in partition:
                raw_val = row.value
                if raw_val is None:
                    continue
                rb = raw_val if isinstance(raw_val, (bytes, bytearray)) else bytes(raw_val)
                topic, part, off, mkey = _meta(row)
                key_ps = (topic, part)
                st = partition_state.setdefault(
                    key_ps,
                    {"read": 0, "ok": 0, "fail": 0, "parse_errors": 0, "max_o": -1, "last_succ": None, "last_err": None},
                )
                st["read"] += 1
                st["max_o"] = max(st["max_o"], off)

                try:
                    envelope = json.loads(rb.decode("utf-8"))
                except json.JSONDecodeError as exc:
                    st["fail"] += 1
                    st["parse_errors"] += 1
                    st["last_err"] = off
                    log_l.warning(
                        "normalize_wind_json_error",
                        extra={"partition": part, "offset": off, "error": str(exc)},
                    )
                    handle_normalize_failure(
                        conn=conn,
                        producer=producer,
                        topics=topics,
                        raw_bytes=rb,
                        exc=exc,
                        source_topic=source_topic,
                        target_dataset=target_dataset,
                        consumer_group=consumer_group,
                        specific_dlq_topic=topics["wind_dlq_topic"],
                        partition=part,
                        offset_value=off,
                        message_key=mkey,
                    )
                    continue

                try:
                    src = str(envelope.get("source") or "unknown")
                    normalized = envelope["record"]["normalized"]
                except (KeyError, TypeError) as exc:
                    st["fail"] += 1
                    st["parse_errors"] += 1
                    st["last_err"] = off
                    log_l.warning(
                        "normalize_wind_envelope_error",
                        extra={"partition": part, "offset": off, "error": str(exc)},
                    )
                    handle_normalize_failure(
                        conn=conn,
                        producer=producer,
                        topics=topics,
                        raw_bytes=rb,
                        exc=exc,
                        source_topic=source_topic,
                        target_dataset=target_dataset,
                        consumer_group=consumer_group,
                        specific_dlq_topic=topics["wind_dlq_topic"],
                        partition=part,
                        offset_value=off,
                        message_key=mkey,
                    )
                    continue

                try:
                    cur.execute("SAVEPOINT wind_norm_row")
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
                    cur.execute("RELEASE SAVEPOINT wind_norm_row")
                    st["ok"] += 1
                    st["last_succ"] = off
                except Exception as exc:  # noqa: BLE001
                    cur.execute("ROLLBACK TO SAVEPOINT wind_norm_row")
                    st["fail"] += 1
                    st["parse_errors"] += 1
                    st["last_err"] = off
                    log_l.warning(
                        "normalize_wind_row_error",
                        extra={"partition": part, "offset": off, "error": str(exc)},
                    )
                    handle_normalize_failure(
                        conn=conn,
                        producer=producer,
                        topics=topics,
                        raw_bytes=rb,
                        exc=exc,
                        source_topic=source_topic,
                        target_dataset=target_dataset,
                        consumer_group=consumer_group,
                        specific_dlq_topic=topics["wind_dlq_topic"],
                        partition=part,
                        offset_value=off,
                        message_key=mkey,
                    )

        for (topic, part), st in partition_state.items():
            if st["max_o"] < 0:
                continue
            upsert_consumer_offset(
                conn,
                consumer_group=consumer_group,
                topic=topic,
                partition=part,
                current_offset=st["max_o"],
                last_successful_offset=st["last_succ"],
                last_error_offset=st["last_err"],
                metadata={
                    "records_read": st["read"],
                    "records_written": st["ok"],
                    "records_failed": st["fail"],
                    "parse_error_count": st["parse_errors"],
                    "target_dataset": target_dataset,
                },
            )

        conn.commit()
        producer.flush()

        log_l.info(
            "normalize_wind_partition_summary",
            extra={
                "consumer_group": consumer_group,
                "records_read": sum(s["read"] for s in partition_state.values()),
                "records_written": sum(s["ok"] for s in partition_state.values()),
                "records_failed": sum(s["fail"] for s in partition_state.values()),
                "parse_error_count": sum(s["parse_errors"] for s in partition_state.values()),
            },
        )
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
