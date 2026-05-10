from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from pyspark.sql import SparkSession

from wildfire_smoke.db.connection import connect
from wildfire_smoke.db.spatial import associate_grid_weather_points
from wildfire_smoke.grid_weather_records import cells_from_envelope_record, parse_iso_datetime
from wildfire_smoke.settings import kafka_topics


def _foreach_partition(partition: Iterable) -> None:
    import psycopg
    from kafka import KafkaProducer

    from wildfire_smoke.dlq import handle_normalize_failure, upsert_consumer_offset

    log_l = logging.getLogger(__name__)
    topics = kafka_topics()
    conninfo = os.environ["PSYCOPG_CONNINFO"]
    kafka_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    consumer_group = os.environ.get("NORMALIZER_CONSUMER_GROUP", "spark-normalize-grid-weather")
    source_topic = topics["grid_weather_raw_topic"]
    target_dataset = "normalized.weather_grid_cells"

    conn = psycopg.connect(conninfo)
    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in kafka_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    insert_raw = """
        INSERT INTO raw.gridded_weather (source, grid_id, valid_time, payload)
        VALUES (%s, %s, %s, %s::jsonb)
        """

    upsert_norm = """
        INSERT INTO normalized.weather_grid_cells (
          weather_cell_id, source, grid_id, valid_time, forecast_time,
          latitude, longitude, wind_speed_mps, wind_direction_degrees,
          temperature_c, relative_humidity_percent, geom
        ) VALUES (
          %s, %s, %s, %s::timestamptz, %s::timestamptz,
          %s, %s, %s, %s, %s, %s,
          ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        )
        ON CONFLICT (weather_cell_id) DO UPDATE SET
          source = EXCLUDED.source,
          grid_id = EXCLUDED.grid_id,
          valid_time = EXCLUDED.valid_time,
          forecast_time = EXCLUDED.forecast_time,
          latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude,
          wind_speed_mps = EXCLUDED.wind_speed_mps,
          wind_direction_degrees = EXCLUDED.wind_direction_degrees,
          temperature_c = EXCLUDED.temperature_c,
          relative_humidity_percent = EXCLUDED.relative_humidity_percent,
          geom = EXCLUDED.geom;
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
                    handle_normalize_failure(
                        conn=conn,
                        producer=producer,
                        topics=topics,
                        raw_bytes=rb,
                        exc=exc,
                        source_topic=source_topic,
                        target_dataset=target_dataset,
                        consumer_group=consumer_group,
                        specific_dlq_topic=topics["grid_weather_dlq_topic"],
                        partition=part,
                        offset_value=off,
                        message_key=mkey,
                    )
                    continue

                try:
                    src = str(envelope.get("source") or "unknown")
                    grid_id = envelope.get("grid_id")
                    grid_id_s = str(grid_id) if grid_id is not None else None
                    vt = parse_iso_datetime(envelope.get("valid_time"))
                    if vt is None:
                        raise ValueError("missing valid_time")
                    record = envelope["record"]
                    if not isinstance(record, dict):
                        raise ValueError("record must be object")
                    cells = cells_from_envelope_record(record, source=src, grid_id=grid_id_s, valid_time=vt)
                except (KeyError, TypeError, ValueError) as exc:
                    st["fail"] += 1
                    st["parse_errors"] += 1
                    st["last_err"] = off
                    handle_normalize_failure(
                        conn=conn,
                        producer=producer,
                        topics=topics,
                        raw_bytes=rb,
                        exc=exc,
                        source_topic=source_topic,
                        target_dataset=target_dataset,
                        consumer_group=consumer_group,
                        specific_dlq_topic=topics["grid_weather_dlq_topic"],
                        partition=part,
                        offset_value=off,
                        message_key=mkey,
                    )
                    continue

                try:
                    cur.execute("SAVEPOINT grid_norm_row")
                    cur.execute(
                        insert_raw,
                        (src, grid_id_s, vt, json.dumps(envelope)),
                    )
                    for nc in cells:
                        cur.execute(
                            upsert_norm,
                            (
                                nc["weather_cell_id"],
                                nc["source"],
                                nc["grid_id"],
                                nc["valid_time"],
                                nc["forecast_time"],
                                nc["latitude"],
                                nc["longitude"],
                                nc["wind_speed_mps"],
                                nc["wind_direction_degrees"],
                                nc["temperature_c"],
                                nc["relative_humidity_percent"],
                                nc["longitude"],
                                nc["latitude"],
                            ),
                        )
                        producer.send(topics["grid_weather_normalized_topic"], value=nc)
                    cur.execute("RELEASE SAVEPOINT grid_norm_row")
                    st["ok"] += 1
                    st["last_succ"] = off
                except Exception as exc:  # noqa: BLE001
                    cur.execute("ROLLBACK TO SAVEPOINT grid_norm_row")
                    st["fail"] += 1
                    st["parse_errors"] += 1
                    st["last_err"] = off
                    log_l.warning(
                        "normalize_grid_weather_row_error",
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
                        specific_dlq_topic=topics["grid_weather_dlq_topic"],
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
            "normalize_grid_weather_partition_summary",
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

    spark = SparkSession.builder.appName("normalize-grid-weather").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topics["grid_weather_raw_topic"])
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    df.foreachPartition(_foreach_partition)

    with connect() as conn:
        associate_grid_weather_points(conn)
        conn.commit()

    spark.stop()


if __name__ == "__main__":
    main()
