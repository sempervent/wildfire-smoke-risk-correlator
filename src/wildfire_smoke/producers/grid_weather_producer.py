"""Bounded gridded weather producer → Kafka ``weather.grid.raw`` (Phase 9–10)."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer

from wildfire_smoke.grid_weather_provider import attach_batch_metadata, grid_weather_provider_for_settings
from wildfire_smoke.settings import Settings, kafka_topics

log = logging.getLogger(__name__)


def build_envelope(
    *,
    source: str,
    grid_id: str | None,
    valid_time: datetime,
    cells: list[dict[str, Any]],
    fetched_at: datetime | None = None,
) -> dict[str, Any]:
    ft = fetched_at or datetime.now(timezone.utc).replace(microsecond=0)
    return {
        "source": source,
        "fetched_at": ft.isoformat(),
        "grid_id": grid_id,
        "valid_time": valid_time.isoformat(),
        "record": {"cells": cells},
    }


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    topics = kafka_topics()
    raw_topic = topics["grid_weather_raw_topic"]

    provider = grid_weather_provider_for_settings(settings)
    batch = provider.fetch_batch()

    env = build_envelope(
        source=settings.grid_weather_source,
        grid_id=batch.grid_id,
        valid_time=batch.valid_time,
        cells=batch.cells,
    )
    attach_batch_metadata(env, batch)

    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in settings.kafka_bootstrap_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    try:
        producer.send(raw_topic, value=env)
        producer.flush()
    finally:
        producer.close()

    log.info("grid_weather_publish_complete", extra={"topic": raw_topic, "cells": len(batch.cells)})


if __name__ == "__main__":
    main()
