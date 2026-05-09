"""Publish malformed fixture rows to raw Kafka topics (no API keys)."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

from wildfire_smoke.settings import Settings, kafka_topics, repo_root

log = logging.getLogger(__name__)


def _producer(settings: Settings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[s.strip() for s in settings.kafka_bootstrap_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )


def _resolve(p: Path) -> Path:
    return p if p.is_absolute() else repo_root() / p


def publish_firms_bad(settings: Settings, producer: KafkaProducer, topics: dict[str, str]) -> int:
    path = _resolve(Path(os.environ.get("FIRMS_BAD_FIXTURE_CSV", "tests/fixtures/firms_bad_sample.csv")))
    text = path.read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(text))
    fetched_at = datetime.now(timezone.utc).isoformat()
    n = 0
    for row in reader:
        envelope = {
            "source": settings.firms_source,
            "fetched_at": fetched_at,
            "api_url_without_secret": f"file://{path}",
            "record": dict(row),
        }
        producer.send(topics["firms_raw_topic"], value=envelope)
        n += 1
    return n


def publish_openaq_bad(producer: KafkaProducer, topics: dict[str, str]) -> int:
    path = _resolve(Path(os.environ.get("OPENAQ_BAD_FIXTURE_JSONL", "tests/fixtures/openaq_bad_sample.jsonl")))
    n = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            producer.send(topics["openaq_raw_topic"], value=obj)
        except json.JSONDecodeError:
            producer.send(topics["openaq_raw_topic"], value={"_invalid_json_line": line[:4000]})
        n += 1
    return n


def publish_wind_bad(producer: KafkaProducer, topics: dict[str, str]) -> int:
    path = _resolve(Path(os.environ.get("WIND_BAD_FIXTURE_JSONL", "tests/fixtures/wind_bad_sample.jsonl")))
    n = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            producer.send(topics["wind_raw_topic"], value=obj)
        except json.JSONDecodeError:
            producer.send(topics["wind_raw_topic"], value={"_invalid_json_line": line[:4000]})
        n += 1
    return n


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    topics = kafka_topics()
    producer = _producer(settings)
    try:
        fc = publish_firms_bad(settings, producer, topics)
        oc = publish_openaq_bad(producer, topics)
        wc = publish_wind_bad(producer, topics)
        producer.flush()
        log.info(
            "replay_bad_fixtures_complete",
            extra={"firms_messages": fc, "openaq_messages": oc, "wind_messages": wc},
        )
    finally:
        producer.close()


if __name__ == "__main__":
    main()
