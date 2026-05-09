from __future__ import annotations

import csv
import hashlib
import io
from datetime import datetime, timezone
from typing import Any


def detection_id(source: str, latitude: float, longitude: float, acq_datetime: datetime) -> str:
    acq_utc = acq_datetime.astimezone(timezone.utc).isoformat()
    base = f"{source}|{latitude:.6f}|{longitude:.6f}|{acq_utc}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _parse_acq_datetime(record: dict[str, Any]) -> datetime:
    acq_date = str(record.get("acq_date") or "").strip()
    acq_time = str(record.get("acq_time") or "").strip()

    if not acq_date:
        raise ValueError("FIRMS record missing acq_date")

    dt: datetime | None = None
    if "-" in acq_date:
        # yyyy-mm-dd
        date_part = datetime.strptime(acq_date, "%Y-%m-%d").date()
    elif len(acq_date) == 8 and acq_date.isdigit():
        date_part = datetime.strptime(acq_date, "%Y%m%d").date()
    else:
        raise ValueError(f"Unrecognized acq_date format: {acq_date!r}")

    if acq_time:
        acq_time_digits = "".join(ch for ch in acq_time if ch.isdigit())
        if len(acq_time_digits) >= 4:
            hh = int(acq_time_digits[0:2])
            mm = int(acq_time_digits[2:4])
            ss = int(acq_time_digits[4:6]) if len(acq_time_digits) >= 6 else 0
            dt = datetime(date_part.year, date_part.month, date_part.day, hh, mm, ss, tzinfo=timezone.utc)
        else:
            raise ValueError(f"Unrecognized acq_time format: {acq_time!r}")
    else:
        dt = datetime(date_part.year, date_part.month, date_part.day, tzinfo=timezone.utc)

    return dt


def parse_firms_csv_text(csv_text: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(csv_text))
    return [dict(row) for row in reader]


def normalized_fire_fields(source: str, record: dict[str, Any]) -> dict[str, Any]:
    lat = float(record["latitude"])
    lon = float(record["longitude"])
    acq = _parse_acq_datetime(record)

    confidence = record.get("confidence")
    brightness_raw = record.get("brightness") or record.get("bright_t31")
    brightness = float(brightness_raw) if brightness_raw not in (None, "",) else None

    frp_raw = record.get("frp")
    frp = float(frp_raw) if frp_raw not in (None, "",) else None

    daynight = record.get("daynight")

    det_id = detection_id(source, lat, lon, acq)

    return {
        "detection_id": det_id,
        "source": source,
        "latitude": lat,
        "longitude": lon,
        "acq_datetime": acq,
        "confidence": None if confidence in (None, "") else str(confidence),
        "brightness": brightness,
        "frp": frp,
        "daynight": None if daynight in (None, "") else str(daynight),
    }
