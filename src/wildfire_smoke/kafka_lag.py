"""Collect broker watermark + approximate consumer lag observations into Postgres."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from kafka import KafkaConsumer, TopicPartition

from wildfire_smoke.db.connection import connect
from wildfire_smoke.settings import Settings, kafka_topics

log = logging.getLogger(__name__)

INSERT_TOPIC_OFFSET = """
INSERT INTO analytics.kafka_topic_offsets (topic, partition, low_watermark, high_watermark, metadata)
VALUES (%s, %s, %s, %s, %s::jsonb)
"""

INSERT_LAG_OBS = """
INSERT INTO analytics.kafka_consumer_lag_observations (
  consumer_group, topic, partition, current_offset, high_watermark, lag, metadata
) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
"""


def monitored_topics() -> list[str]:
    t = kafka_topics()
    return sorted(
        {
            t["firms_raw_topic"],
            t["openaq_raw_topic"],
            t["wind_raw_topic"],
            t["grid_weather_raw_topic"],
            t["firms_dlq_topic"],
            t["openaq_dlq_topic"],
            t["wind_dlq_topic"],
            t["grid_weather_dlq_topic"],
            t["normalization_errors_topic"],
            t["fire_normalized_topic"],
            t["air_quality_normalized_topic"],
            t["wind_normalized_topic"],
            t["grid_weather_normalized_topic"],
            t["smoke_risk_topic"],
        }
    )


def _end_offsets(bootstrap: str, topics: list[str]) -> dict[tuple[str, int], int]:
    consumer = KafkaConsumer(
        bootstrap_servers=[s.strip() for s in bootstrap.split(",") if s.strip()],
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )
    highs: dict[tuple[str, int], int] = {}
    try:
        consumer.poll(timeout_ms=1000)
        for topic in topics:
            parts = consumer.partitions_for_topic(topic)
            if not parts:
                continue
            tps = [TopicPartition(topic, p) for p in sorted(parts)]
            ends = consumer.end_offsets(tps)
            for tp, hi in ends.items():
                highs[(tp.topic, tp.partition)] = int(hi)
    finally:
        consumer.close()
    return highs


def _fetch_app_offsets(conn: Any) -> list[tuple[str, str, int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT consumer_group, topic, partition, current_offset
            FROM analytics.kafka_consumer_offsets
            WHERE consumer_group LIKE 'spark-normalize%'
            """
        )
        return [(str(r[0]), str(r[1]), int(r[2]), int(r[3])) for r in cur.fetchall()]


def collect_and_store_lag(settings: Settings) -> dict[str, int]:
    bootstrap = settings.kafka_bootstrap_servers
    topics = monitored_topics()
    meta_collect = {"collector": "wildfire_smoke.kafka_lag", "topics_count": len(topics)}
    highs = _end_offsets(bootstrap, topics)

    topic_rows = 0
    lag_rows = 0
    with connect(settings) as conn:
        with conn.cursor() as cur:
            for (topic, partition), hi in sorted(highs.items()):
                cur.execute(
                    INSERT_TOPIC_OFFSET,
                    (topic, partition, None, hi, json.dumps({**meta_collect, "topic": topic})),
                )
                topic_rows += 1

            app_offsets = _fetch_app_offsets(conn)
            for consumer_group, topic, partition, cur_off in app_offsets:
                key = (topic, partition)
                hi = highs.get(key)
                if hi is None:
                    continue
                lag = max(0, int(hi) - int(cur_off))
                cur.execute(
                    INSERT_LAG_OBS,
                    (
                        consumer_group,
                        topic,
                        partition,
                        cur_off,
                        hi,
                        lag,
                        json.dumps(
                            {
                                "lag_semantics": "high_watermark_minus_application_current_offset",
                                "note": "Approximate; compare with broker consumer commits when available.",
                            }
                        ),
                    ),
                )
                lag_rows += 1
        conn.commit()

    log.info(
        "kafka_lag_collection_complete",
        extra={"topic_offset_rows": topic_rows, "lag_observation_rows": lag_rows},
    )
    return {"topic_offset_rows": topic_rows, "lag_observation_rows": lag_rows}


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    stats = collect_and_store_lag(Settings.from_env())
    print(json.dumps(stats))


if __name__ == "__main__":
    main()
