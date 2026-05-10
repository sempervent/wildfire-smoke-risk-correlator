from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


def measurement_id(sensor_id: str, measured_at: datetime, parameter: str) -> str:
    base = f"{sensor_id}|{measured_at.astimezone(timezone.utc).isoformat()}|{parameter.lower()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def parse_openaq_datetime(value: str) -> datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalized_measurement_fields(
    *,
    provider: str | None,
    location_id: str | None,
    sensor_id: str,
    parameter: str,
    value: float,
    unit: str,
    measured_at: datetime,
    latitude: float,
    longitude: float,
) -> dict[str, Any]:
    mid = measurement_id(sensor_id, measured_at, parameter)
    return {
        "measurement_id": mid,
        "provider": provider,
        "location_id": location_id,
        "sensor_id": sensor_id,
        "parameter": parameter.lower(),
        "value": float(value),
        "unit": unit,
        "measured_at": measured_at.astimezone(timezone.utc),
        "latitude": float(latitude),
        "longitude": float(longitude),
    }
