from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import httpx
from kafka import KafkaProducer

from wildfire_smoke import firms_csv
from wildfire_smoke.logging import configure_logging
from wildfire_smoke.settings import Settings, kafka_topics, load_yaml_config, repo_root

log = logging.getLogger(__name__)


def _resolve_path(p: Path) -> Path:
    return p if p.is_absolute() else repo_root() / p


def _producer(settings: Settings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[s.strip() for s in settings.kafka_bootstrap_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )


def _publish_deadletter(producer: KafkaProducer, topic_dlq: str, payload: dict, settings: Settings) -> None:
    producer.send(topic_dlq, value=payload)
    producer.flush()


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    topics = kafka_topics()

    if not settings.firms_dry_run and not settings.firms_map_key:
        raise RuntimeError(
            "FIRMS_MAP_KEY is required for live FIRMS ingestion. "
            "Use FIRMS_DRY_RUN=1 with FIRMS_FIXTURE_CSV for offline fixture publishing."
        )

    producer = _producer(settings)

    if settings.firms_dry_run:
        fixture = _resolve_path(settings.firms_fixture_csv)
        if not fixture.exists():
            raise FileNotFoundError(f"FIRMS fixture CSV not found: {fixture}")
        csv_text = fixture.read_text(encoding="utf-8")
        fetched_at = datetime.now(timezone.utc).isoformat()
        api_url_without_secret = f"file://{fixture}"
        log.info(
            "firms_dry_run_enabled",
            extra={"fixture": str(fixture)},
        )
    else:
        map_key = settings.firms_map_key
        assert map_key is not None
        base = str(load_yaml_config("sources.yaml")["firms"]["base_url"])

        url = (
            f"{base}/{map_key}/"
            f"{settings.firms_source}/{settings.firms_bbox}/{settings.firms_day_range}"
        )
        api_url_without_secret = url.replace(map_key, "***")

        log.info("firms_fetch_start", extra={"api_url_without_secret": api_url_without_secret})
        with httpx.Client(timeout=120.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            csv_text = resp.text
        fetched_at = datetime.now(timezone.utc).isoformat()

    rows = firms_csv.parse_firms_csv_text(csv_text)
    log.info("firms_rows_parsed", extra={"count": len(rows)})

    sent = 0
    for row in rows:
        envelope = {
            "source": settings.firms_source,
            "fetched_at": fetched_at,
            "api_url_without_secret": api_url_without_secret,
            "record": row,
        }
        try:
            producer.send(topics["firms_raw_topic"], value=envelope)
            sent += 1
        except Exception as exc:
            log.exception("firms_publish_failed", extra={"error": str(exc)})
            _publish_deadletter(
                producer,
                topics["deadletter_topic"],
                {"source": "firms_producer", "error": str(exc), "envelope": envelope},
                settings,
            )
            raise

    producer.flush()
    log.info("firms_publish_complete", extra={"sent": sent})


if __name__ == "__main__":
    main()
