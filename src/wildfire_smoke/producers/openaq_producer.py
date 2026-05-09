from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from kafka import KafkaProducer

from wildfire_smoke.logging import configure_logging
from wildfire_smoke.openaq_records import normalized_measurement_fields, parse_openaq_datetime
from wildfire_smoke.settings import Settings, kafka_topics, load_yaml_config, repo_root

log = logging.getLogger(__name__)


def _resolve_path(p: Path) -> Path:
    return p if p.is_absolute() else repo_root() / p


def _producer(settings: Settings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[s.strip() for s in settings.kafka_bootstrap_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )


def _headers(settings: Settings) -> dict[str, str]:
    headers: dict[str, str] = {}
    if settings.openaq_api_key:
        headers["X-API-Key"] = settings.openaq_api_key
    return headers


def _sleep_backoff(attempt: int, base: float) -> None:
    delay = min(60.0, base ** attempt)
    time.sleep(delay)


def _request(client: httpx.Client, method: str, url: str, *, headers: dict[str, str], **kwargs: Any) -> httpx.Response:
    cfg = load_yaml_config("sources.yaml")["openaq"]
    max_retries = int(cfg["max_retries"])
    base_backoff = float(cfg["backoff_seconds_base"])
    timeout = float(cfg["http_timeout_seconds"])

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.request(method, url, headers=headers, timeout=timeout, **kwargs)
            if resp.status_code == 429:
                log.warning("openaq_rate_limited", extra={"attempt": attempt, "url": url})
                _sleep_backoff(attempt, base_backoff)
                continue
            if 500 <= resp.status_code < 600:
                log.warning(
                    "openaq_server_error",
                    extra={"attempt": attempt, "status_code": resp.status_code, "url": url},
                )
                _sleep_backoff(attempt, base_backoff)
                continue
            return resp
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last_exc = exc
            log.warning("openaq_transport_error", extra={"attempt": attempt, "error": str(exc), "url": url})
            _sleep_backoff(attempt, base_backoff)

    if last_exc:
        raise last_exc
    raise RuntimeError(f"OpenAQ request failed after retries: {url}")


def _parameter_name(parameter_id: int) -> str:
    # IDs are configurable via sources.yaml; names are normalized for joins/scoring.
    if parameter_id == 2:
        return "pm25"
    if parameter_id == 3:
        return "pm10"
    return f"param_{parameter_id}"


def _iter_locations_pages(
    client: httpx.Client,
    *,
    base_url: str,
    headers: dict[str, str],
    bbox: str,
    parameter_id: int,
    limit: int,
    max_pages: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/locations"
        resp = _request(
            client,
            "GET",
            url,
            headers=headers,
            params={
                "bbox": bbox,
                "parameters_id": parameter_id,
                "limit": limit,
                "page": page,
            },
        )
        if resp.status_code in {401, 403}:
            raise RuntimeError(
                "OpenAQ returned "
                f"{resp.status_code}. Set OPENAQ_API_KEY if required for your environment."
            )
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            break
        out.extend(results)

        meta = payload.get("meta") or {}
        found_raw = meta.get("found")
        found: int | None
        try:
            found = int(found_raw) if found_raw is not None else None
        except (TypeError, ValueError):
            found = None

        if found is not None and page * limit >= found:
            break
        page += 1
        time.sleep(0.15)

    return out


def _iter_sensor_measurements(
    client: httpx.Client,
    *,
    base_url: str,
    headers: dict[str, str],
    sensor_id: str,
    measurements_limit: int,
    max_pages: int,
    datetime_min: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/sensors/{sensor_id}/measurements"
        resp = _request(
            client,
            "GET",
            url,
            headers=headers,
            params={
                "datetime_min": datetime_min,
                "limit": measurements_limit,
                "page": page,
            },
        )
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            break
        out.extend(results)

        meta = payload.get("meta") or {}
        found_raw = meta.get("found")
        found: int | None
        try:
            found = int(found_raw) if found_raw is not None else None
        except (TypeError, ValueError):
            found = None

        if found is not None and page * measurements_limit >= found:
            break
        page += 1
        time.sleep(0.15)

    return out


def _coords_from_location(location: dict[str, Any]) -> tuple[float, float]:
    coords = location.get("coordinates") or {}
    lat = coords.get("latitude")
    lon = coords.get("longitude")
    if lat is None or lon is None:
        raise ValueError(f"Location missing coordinates: id={location.get('id')}")
    return float(lat), float(lon)


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    topics = kafka_topics()
    cfg = load_yaml_config("sources.yaml")["openaq"]

    producer = _producer(settings)

    if settings.openaq_dry_run:
        fixture = _resolve_path(settings.openaq_fixture_jsonl)
        if not fixture.exists():
            raise FileNotFoundError(f"OpenAQ fixture JSONL not found: {fixture}")
        fetched_at = datetime.now(timezone.utc).isoformat()
        log.info("openaq_dry_run_enabled", extra={"fixture": str(fixture)})
        sent = 0
        for line in fixture.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            envelope = json.loads(line)
            producer.send(topics["openaq_raw_topic"], value=envelope)
            sent += 1
        producer.flush()
        log.info("openaq_publish_complete", extra={"sent": sent})
        return

    base_url = str(cfg["base_url"]).rstrip("/")
    headers = _headers(settings)

    hours_back = int(cfg["measurements_hours_back"])
    datetime_min = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")

    locations_limit = int(cfg["locations_limit"])
    measurements_limit = int(cfg["measurements_limit"])
    max_pages = int(cfg["max_pages"])
    parameter_ids = [int(x) for x in cfg["parameter_ids"]]

    fetched_at = datetime.now(timezone.utc).isoformat()

    with httpx.Client() as client:
        for parameter_id in parameter_ids:
            pname = _parameter_name(parameter_id)
            locations = _iter_locations_pages(
                client,
                base_url=base_url,
                headers=headers,
                bbox=settings.openaq_bbox,
                parameter_id=parameter_id,
                limit=locations_limit,
                max_pages=max_pages,
            )
            log.info(
                "openaq_locations_fetched",
                extra={"parameter": pname, "parameter_id": parameter_id, "count": len(locations)},
            )

            for loc in locations:
                location_id = str(loc.get("id"))

                lat, lon = _coords_from_location(loc)

                sensors = loc.get("sensors") or []
                matching_sensor_ids: list[str] = []
                for sensor in sensors:
                    param = sensor.get("parameter") or {}
                    if int(param.get("id")) != parameter_id:
                        continue
                    matching_sensor_ids.append(str(sensor["id"]))

                for sensor_id in matching_sensor_ids:
                    measurements = _iter_sensor_measurements(
                        client,
                        base_url=base_url,
                        headers=headers,
                        sensor_id=sensor_id,
                        measurements_limit=measurements_limit,
                        max_pages=max_pages,
                        datetime_min=datetime_min,
                    )

                    for m in measurements:
                        period = m.get("period") or {}
                        dt_obj = period.get("datetimeFrom") or period.get("datetime_to") or period.get("datetimeTo")
                        if isinstance(dt_obj, dict):
                            dt_raw = dt_obj.get("utc") or dt_obj.get("local")
                        else:
                            dt_raw = dt_obj
                        if not dt_raw:
                            raise ValueError(f"Measurement missing datetime: sensor={sensor_id} payload={m}")
                        measured_at = parse_openaq_datetime(str(dt_raw))

                        value_raw = m.get("value")
                        if value_raw is None:
                            continue

                        param_obj = m.get("parameter") or {}
                        unit = str(param_obj.get("units") or "")

                        normalized = normalized_measurement_fields(
                            provider="openaq",
                            location_id=location_id,
                            sensor_id=sensor_id,
                            parameter=pname,
                            value=float(value_raw),
                            unit=unit or "unknown",
                            measured_at=measured_at,
                            latitude=lat,
                            longitude=lon,
                        )

                        record = {
                            "location": {"id": location_id, "coordinates": {"latitude": lat, "longitude": lon}},
                            "sensor_id": sensor_id,
                            "parameter": pname,
                            "measurement": m,
                            "normalized": normalized,
                        }

                        envelope = {
                            "source": "openaq",
                            "fetched_at": fetched_at,
                            "parameter": pname,
                            "record": record,
                        }
                        producer.send(topics["openaq_raw_topic"], value=envelope)

    producer.flush()
    log.info("openaq_publish_complete")


if __name__ == "__main__":
    main()
