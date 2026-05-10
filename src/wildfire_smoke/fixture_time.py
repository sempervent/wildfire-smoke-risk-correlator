"""Optional relative rewriting of fixture timestamps for integration demos (Phase 10).

Never mutates files on disk — only in-memory payloads sent to Kafka.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from wildfire_smoke.firms_csv import firms_acquisition_datetime
from wildfire_smoke.openaq_records import parse_openaq_datetime


def normalize_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_shift_to_anchor(
    anchor_times: list[datetime],
    *,
    base_hours_ago: float,
    now: datetime | None = None,
) -> timedelta:
    """Shift so ``max(anchor_times)`` maps to ``now - base_hours_ago``."""

    now = now or datetime.now(timezone.utc)
    if not anchor_times:
        return timedelta(0)
    target = now - timedelta(hours=base_hours_ago)
    latest = max(normalize_utc(t) for t in anchor_times)
    return target - latest


def rewrite_firms_rows(rows: list[dict[str, Any]], shift: timedelta) -> list[str]:
    """Rewrite acq_date/acq_time in place; returns parallel original ISO timestamps."""

    originals: list[str] = []
    for row in rows:
        dt = normalize_utc(firms_acquisition_datetime(row))
        originals.append(dt.isoformat())
        new_dt = dt + shift
        row["acq_date"] = new_dt.strftime("%Y%m%d")
        row["acq_time"] = new_dt.strftime("%H%M")
    return originals


def attach_fixture_time_metadata(
    envelope: dict[str, Any],
    *,
    original_observed_at: str | None,
    rewritten: bool,
) -> None:
    envelope["fixture_time_rewritten"] = rewritten
    if original_observed_at is not None:
        envelope["original_observed_at"] = original_observed_at


def rewrite_openaq_envelope(envelope: dict[str, Any], shift: timedelta) -> str | None:
    """Rewrite normalized measured_at and period UTC strings; returns original measured_at ISO."""

    record = envelope.get("record")
    if not isinstance(record, dict):
        return None
    norm = record.get("normalized")
    if not isinstance(norm, dict):
        return None
    raw_mt = norm.get("measured_at")
    if not raw_mt:
        return None
    dt = normalize_utc(parse_openaq_datetime(str(raw_mt)))
    orig = dt.isoformat()
    new_dt = dt + shift
    norm["measured_at"] = new_dt.isoformat()

    meas = record.get("measurement")
    if isinstance(meas, dict):
        period = meas.get("period")
        if isinstance(period, dict):
            df = period.get("datetimeFrom")
            dt_to = period.get("datetimeTo")
            if isinstance(df, dict) and df.get("utc"):
                u = normalize_utc(parse_openaq_datetime(str(df["utc"]))) + shift
                df["utc"] = u.strftime("%Y-%m-%dT%H:%M:%SZ")
            if isinstance(dt_to, dict) and dt_to.get("utc"):
                u = normalize_utc(parse_openaq_datetime(str(dt_to["utc"]))) + shift
                dt_to["utc"] = u.strftime("%Y-%m-%dT%H:%M:%SZ")
    return orig


def rewrite_wind_json_object(obj: dict[str, Any], shift: timedelta) -> str | None:
    raw = obj.get("observed_at")
    if not raw:
        return None
    dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    dt = normalize_utc(dt)
    orig = dt.isoformat()
    new_dt = dt + shift
    obj["observed_at"] = new_dt.strftime("%Y-%m-%dT%H:%M:%S") + "+00:00"
    return orig


def rewrite_grid_weather_dict(data: dict[str, Any], shift: timedelta) -> str | None:
    vt_raw = data.get("valid_time")
    if not vt_raw:
        return None
    dt = datetime.fromisoformat(str(vt_raw).replace("Z", "+00:00"))
    dt = normalize_utc(dt)
    orig = dt.isoformat()
    new_dt = dt + shift
    data["valid_time"] = new_dt.isoformat().replace("+00:00", "+00:00")

    for cell in data.get("cells") or []:
        if not isinstance(cell, dict):
            continue
        for key in ("forecast_time", "forecastTime"):
            raw = cell.get(key)
            if not raw:
                continue
            cdt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            cdt = normalize_utc(cdt)
            cell[key] = (cdt + shift).isoformat()
    return orig


def collect_openaq_measurement_times(envelopes: list[dict[str, Any]]) -> list[datetime]:
    times: list[datetime] = []
    for env in envelopes:
        rec = env.get("record")
        if not isinstance(rec, dict):
            continue
        norm = rec.get("normalized")
        if not isinstance(norm, dict):
            continue
        mt = norm.get("measured_at")
        if mt:
            times.append(normalize_utc(parse_openaq_datetime(str(mt))))
    return times
