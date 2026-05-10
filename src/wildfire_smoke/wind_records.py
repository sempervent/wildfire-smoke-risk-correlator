"""Parse wind observation payloads used by producers and normalization."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def _parse_ts(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        return raw
    if raw is None:
        raise ValueError("observed_at is required")
    text = str(raw).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


def normalized_wind_from_dict(obj: dict[str, Any]) -> dict[str, Any]:
    """
    Validate and normalize a fixture/API-friendly dict into DB + Kafka fields.

    Expected keys: wind_observation_id, source, observed_at, latitude, longitude.
    Optional: station_id, wind_speed_mps, wind_direction_degrees, wind_gust_mps.
    """

    wid = obj.get("wind_observation_id")
    src = obj.get("source")
    if not wid or not src:
        raise ValueError("wind_observation_id and source are required")
    lat = float(obj["latitude"])
    lon = float(obj["longitude"])
    observed_at = _parse_ts(obj["observed_at"])
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    out: dict[str, Any] = {
        "wind_observation_id": str(wid),
        "source": str(src),
        "station_id": str(obj["station_id"]) if obj.get("station_id") not in (None, "") else None,
        "observed_at": observed_at.isoformat(),
        "latitude": lat,
        "longitude": lon,
        "wind_speed_mps": float(obj["wind_speed_mps"]) if obj.get("wind_speed_mps") is not None else None,
        "wind_direction_degrees": float(obj["wind_direction_degrees"])
        if obj.get("wind_direction_degrees") is not None
        else None,
        "wind_gust_mps": float(obj["wind_gust_mps"]) if obj.get("wind_gust_mps") is not None else None,
    }
    return out


def parse_wind_envelope_record(envelope: dict[str, Any]) -> dict[str, Any]:
    """Extract normalized wind dict from a Kafka envelope ``{'record': {'normalized': {...}}}``."""

    try:
        normalized = envelope["record"]["normalized"]
    except (KeyError, TypeError) as exc:
        raise ValueError("envelope missing record.normalized") from exc
    if not isinstance(normalized, dict):
        raise ValueError("record.normalized must be an object")
    return normalized_wind_from_dict(normalized)
