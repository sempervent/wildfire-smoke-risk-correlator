"""Bounded gridded weather producer → Kafka ``weather.grid.raw`` (Phase 9)."""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from kafka import KafkaProducer

from wildfire_smoke.grid_weather_records import parse_iso_datetime, parse_wind_speed_to_mps
from wildfire_smoke.live_bbox import BBox, bbox_allowed_for_live_ingest, parse_bbox
from wildfire_smoke.settings import Settings, kafka_topics, repo_root
from wildfire_smoke.wind_station_discovery import nws_user_agent, warn_if_default_user_agent_live

log = logging.getLogger(__name__)

_CARDINAL_TO_DEG = {
    "N": 0.0,
    "NNE": 22.5,
    "NE": 45.0,
    "ENE": 67.5,
    "E": 90.0,
    "ESE": 112.5,
    "SE": 135.0,
    "SSE": 157.5,
    "S": 180.0,
    "SSW": 202.5,
    "SW": 225.0,
    "WSW": 247.5,
    "W": 270.0,
    "WNW": 292.5,
    "NW": 315.0,
    "NNW": 337.5,
}


def _wind_direction_to_degrees(raw: Any) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip().upper()
    if not s:
        return None
    m = re.match(r"^(\d+(\.\d+)?)\s*°?$", s)
    if m:
        return float(m.group(1))
    return _CARDINAL_TO_DEG.get(s)


def _assert_grid_bbox(bbox: BBox, settings: Settings) -> None:
    allow_large = os.environ.get("LIVE_INGEST_ALLOW_LARGE_BBOX", "0").strip().lower() in {"1", "true", "yes"}
    if allow_large:
        return
    if settings.grid_weather_refuse_large_bbox and not bbox_allowed_for_live_ingest(bbox):
        lim = os.environ.get("LIVE_INGEST_MAX_SPAN_DEG", "14")
        raise ValueError(
            f"GRID_WEATHER_BBOX span too large (max lon/lat span must be <= {lim} degrees); "
            "narrow bbox or set LIVE_INGEST_ALLOW_LARGE_BBOX=1."
        )


def _load_fixture_cells(path: Path) -> tuple[str | None, datetime, list[dict[str, Any]]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    grid_id = str(data["grid_id"]) if data.get("grid_id") else None
    vt = parse_iso_datetime(data.get("valid_time"))
    if vt is None:
        vt = datetime.now(timezone.utc).replace(microsecond=0)
    cells = data.get("cells")
    if not isinstance(cells, list):
        raise ValueError("fixture must contain cells array")
    return grid_id, vt, [c for c in cells if isinstance(c, dict)]


def _live_cells_from_nws_gridpoint(bbox: BBox, settings: Settings) -> tuple[str | None, datetime, list[dict[str, Any]]]:
    warn_if_default_user_agent_live()
    headers = {"User-Agent": nws_user_agent(), "Accept": "application/json"}
    lat = (bbox.min_lat + bbox.max_lat) / 2.0
    lon = (bbox.min_lon + bbox.max_lon) / 2.0
    url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
    with httpx.Client(timeout=60.0) as client:
        pr = client.get(url, headers=headers)
        pr.raise_for_status()
        pj = pr.json()
        fc_url = (pj.get("properties") or {}).get("forecast")
        if not fc_url:
            raise RuntimeError("NWS points response missing forecast URL")
        fr = client.get(str(fc_url), headers=headers)
        fr.raise_for_status()
        fj = fr.json()
    periods = (fj.get("properties") or {}).get("periods") or []
    if not periods:
        raise RuntimeError("NWS forecast has no periods")
    p0 = periods[0]
    ws = parse_wind_speed_to_mps(p0.get("windSpeed"))
    wd = _wind_direction_to_degrees(p0.get("windDirection"))
    temp_c = None
    if p0.get("temperature") is not None and p0.get("temperatureUnit"):
        tval = float(p0["temperature"])
        if str(p0["temperatureUnit"]).upper() == "F":
            temp_c = (tval - 32.0) * 5.0 / 9.0
        else:
            temp_c = tval
    rh = float(p0["relativeHumidity"]) if p0.get("relativeHumidity") is not None else None
    grid_id = str((pj.get("properties") or {}).get("gridId") or "nws-live")
    valid_time = parse_iso_datetime(p0.get("startTime")) or datetime.now(timezone.utc).replace(microsecond=0)
    cell = {
        "latitude": lat,
        "longitude": lon,
        "wind_speed_mps": ws,
        "wind_direction_degrees": wd,
        "temperature_c": temp_c,
        "relative_humidity_percent": rh,
    }
    return grid_id, valid_time, [cell]


def _generate_grid_points(bbox: BBox, *, cell_step: float, max_points: int) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    lat = bbox.min_lat
    while lat <= bbox.max_lat + 1e-9 and len(out) < max_points:
        lon = bbox.min_lon
        while lon <= bbox.max_lon + 1e-9 and len(out) < max_points:
            out.append((lat, lon))
            lon += cell_step
        lat += cell_step
    return out


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

    bbox_raw = settings.grid_weather_bbox
    if not bbox_raw:
        raise ValueError("GRID_WEATHER_BBOX is required for grid weather producer")
    bbox = parse_bbox(bbox_raw)
    _assert_grid_bbox(bbox, settings)

    source = settings.grid_weather_source
    cells: list[dict[str, Any]]
    grid_id: str | None
    valid_time: datetime

    if settings.grid_weather_dry_run:
        path = settings.grid_weather_fixture_json
        if not path.is_absolute():
            path = repo_root() / path
        grid_id, valid_time, cells = _load_fixture_cells(path)
        # Cap cells for bounded ingest
        cells = cells[: settings.grid_weather_max_points]
        log.info("grid_weather_fixture_loaded", extra={"cells": len(cells), "grid_id": grid_id})
    else:
        warn_if_default_user_agent_live()
        # Minimal live: single NWS gridpoint at bbox center (plus optional point lattice without extra HTTP).
        grid_id, valid_time, center_cells = _live_cells_from_nws_gridpoint(bbox, settings)
        pts = _generate_grid_points(bbox, cell_step=settings.grid_weather_cell_size_degrees, max_points=settings.grid_weather_max_points)
        cells = []
        base = center_cells[0]
        for lat, lon in pts:
            c = dict(base)
            c["latitude"] = lat
            c["longitude"] = lon
            cells.append(c)

    env = build_envelope(source=source, grid_id=grid_id, valid_time=valid_time, cells=cells)

    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in settings.kafka_bootstrap_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    try:
        producer.send(raw_topic, value=env)
        producer.flush()
    finally:
        producer.close()

    log.info("grid_weather_publish_complete", extra={"topic": raw_topic, "cells": len(cells)})


if __name__ == "__main__":
    main()
