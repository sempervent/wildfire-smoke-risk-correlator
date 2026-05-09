"""Publish wind observations to ``weather.wind.raw`` (fixture dry-run or bounded NWS adapter)."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from kafka import KafkaProducer

from wildfire_smoke.db.connection import connect
from wildfire_smoke.ingestion_runs import create_run, finish_run
from wildfire_smoke.logging import configure_logging
from wildfire_smoke.settings import Settings, kafka_topics, repo_root
from wildfire_smoke.wind_records import normalized_wind_from_dict

log = logging.getLogger(__name__)

NWS_BASE = "https://api.weather.gov"
NWS_USER_AGENT = os.environ.get(
    "NWS_USER_AGENT",
    "(wildfire-smoke-risk-correlator, github.com/wildfire-smoke-risk-correlator)",
)


def _resolve_path(p: Path) -> Path:
    return p if p.is_absolute() else repo_root() / p


def _producer(settings: Settings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[s.strip() for s in settings.kafka_bootstrap_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )


def _wind_config(settings: Settings, *, fixture_path: str | None) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "wind_source": settings.wind_source,
        "wind_bbox": settings.wind_bbox,
        "wind_station_ids": list(settings.wind_station_ids),
        "wind_dry_run": settings.wind_dry_run,
    }
    if fixture_path is not None:
        cfg["fixture_path"] = fixture_path
    return cfg


def _envelope(source: str, normalized: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": source,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "record": {"normalized": normalized},
    }


def _nws_speed_to_mps(prop: dict[str, Any] | None) -> float | None:
    if not prop or prop.get("value") is None:
        return None
    val = float(prop["value"])
    unit = str(prop.get("unitCode") or "")
    if "knot" in unit.lower() or "KTS" in unit:
        return val * 0.514444
    # NOAA typically labels SI meters per second as unit:m_s-1
    return val


def _nws_direction_degrees(prop: dict[str, Any] | None) -> float | None:
    if not prop or prop.get("value") is None:
        return None
    return float(prop["value"])


def _fetch_station_coords(client: httpx.Client, station_id: str) -> tuple[float, float]:
    url = f"{NWS_BASE}/stations/{station_id}"
    resp = client.get(url, headers={"User-Agent": NWS_USER_AGENT}, timeout=60.0)
    resp.raise_for_status()
    payload = resp.json()
    geom = payload.get("geometry") or {}
    coords = geom.get("coordinates")
    if not coords or len(coords) < 2:
        props = payload.get("properties") or {}
        lat = props.get("latitude")
        lon = props.get("longitude")
        if lat is None or lon is None:
            raise ValueError(f"station {station_id}: missing coordinates")
        return float(lon), float(lat)
    lon, lat = float(coords[0]), float(coords[1])
    return lon, lat


def _fetch_latest_observation(client: httpx.Client, station_id: str) -> dict[str, Any]:
    url = f"{NWS_BASE}/stations/{station_id}/observations/latest"
    resp = client.get(url, headers={"User-Agent": NWS_USER_AGENT}, timeout=60.0)
    resp.raise_for_status()
    return resp.json()


def _normalized_from_nws(station_id: str, station_lon: float, station_lat: float, obs_payload: dict[str, Any]) -> dict[str, Any]:
    props = obs_payload.get("properties") or {}
    ts_raw = props.get("timestamp") or props.get("observed_at")
    if not ts_raw:
        raise ValueError("NWS observation missing timestamp")
    wind_speed_mps = _nws_speed_to_mps(props.get("windSpeed"))
    wind_direction_degrees = _nws_direction_degrees(props.get("windDirection"))
    gust_prop = props.get("windGust")
    wind_gust_mps = _nws_speed_to_mps(gust_prop) if isinstance(gust_prop, dict) else None

    wid = str(props.get("@id") or obs_payload.get("@id") or f"nws-{station_id}-{ts_raw}")
    raw = {
        "wind_observation_id": wid,
        "source": "nws",
        "station_id": station_id,
        "observed_at": str(ts_raw),
        "latitude": station_lat,
        "longitude": station_lon,
        "wind_speed_mps": wind_speed_mps,
        "wind_direction_degrees": wind_direction_degrees,
        "wind_gust_mps": wind_gust_mps,
    }
    return normalized_wind_from_dict(raw)


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    topics = kafka_topics()

    producer = _producer(settings)
    mode = "dry_run" if settings.wind_dry_run else "live"
    fixture_path: str | None = None
    if settings.wind_dry_run:
        fixture_path = str(_resolve_path(settings.wind_fixture_jsonl))

    run_id = None
    sent = 0
    fetched = 0
    records_failed = 0

    try:
        with connect(settings) as conn:
            run_id = create_run(conn, source="wind", mode=mode, config=_wind_config(settings, fixture_path=fixture_path))

        envelopes: list[dict[str, Any]] = []

        if settings.wind_dry_run:
            fixture = _resolve_path(settings.wind_fixture_jsonl)
            if not fixture.exists():
                raise FileNotFoundError(f"WIND fixture JSONL not found: {fixture}")
            log.info("wind_dry_run_enabled", extra={"fixture": str(fixture)})
            for line in fixture.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                normalized = normalized_wind_from_dict(obj)
                envelopes.append(_envelope(str(normalized["source"]), normalized))
                fetched += 1
        else:
            if settings.wind_source not in {"nws"}:
                raise ValueError(f"Unsupported WIND_SOURCE for live mode: {settings.wind_source!r}")

            if not settings.wind_station_ids:
                log.warning(
                    "wind_live_no_stations",
                    extra={
                        "hint": "Set WIND_STATION_IDS (comma-separated ICAO ids, e.g. KTYS,KCHA). "
                        "Bounding-box station discovery from WIND_BBOX is not implemented in v1."
                    },
                )
            else:
                with httpx.Client() as client:
                    for sid in settings.wind_station_ids:
                        try:
                            lon, lat = _fetch_station_coords(client, sid)
                            obs = _fetch_latest_observation(client, sid)
                            normalized = _normalized_from_nws(sid, lon, lat, obs)
                            envelopes.append(_envelope("nws", normalized))
                            fetched += 1
                        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                            records_failed += 1
                            log.warning("wind_station_fetch_failed", extra={"station_id": sid, "error": str(exc)})

        for env in envelopes:
            producer.send(topics["wind_raw_topic"], value=env)
            sent += 1

        producer.flush()

        live_attempted = not settings.wind_dry_run and bool(settings.wind_station_ids)
        ok = settings.wind_dry_run or sent > 0 or not live_attempted
        with connect(settings) as conn:
            finish_run(
                conn,
                run_id,
                status="succeeded" if ok else "failed",
                records_fetched=fetched,
                records_published=sent,
                records_failed=records_failed,
                error_message=None if ok else "no wind observations published",
            )
    except Exception as exc:
        log.exception("wind_producer_failed")
        if run_id is not None:
            with connect(settings) as conn:
                finish_run(
                    conn,
                    run_id,
                    status="failed",
                    records_fetched=fetched,
                    records_published=sent,
                    records_failed=records_failed,
                    error_message=str(exc),
                )
        raise
    finally:
        producer.close()

    log.info(
        "wind_producer_complete",
        extra={"mode": mode, "published": sent, "fetched": fetched, "failed": records_failed},
    )


if __name__ == "__main__":
    main()
