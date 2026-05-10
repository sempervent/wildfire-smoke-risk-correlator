"""Parse gridded weather payloads into normalized cell rows."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


def mph_to_mps(mph: float) -> float:
    return mph * 0.44704


def kmh_to_mps(kmh: float) -> float:
    return kmh / 3.6


def fahrenheit_to_celsius(f: float) -> float:
    return (f - 32.0) * 5.0 / 9.0


_WIND_SPEED_RE = re.compile(r"([\d.]+)\s*(mph|km/h|m/s)?", re.I)


def parse_wind_speed_to_mps(raw: Any) -> float | None:
    """Accept numeric m/s, or strings like '10 mph', '15 km/h'."""

    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    if not s:
        return None
    m = _WIND_SPEED_RE.search(s)
    if not m:
        return None
    val = float(m.group(1))
    unit = (m.group(2) or "m/s").lower()
    if unit == "mph":
        return mph_to_mps(val)
    if unit in {"km/h", "kmh"}:
        return kmh_to_mps(val)
    return val


def parse_temperature_to_celsius(raw: Any, *, unit_hint: str | None = None) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        v = float(raw)
        if unit_hint and unit_hint.lower() in {"degf", "f", "fahrenheit"}:
            return fahrenheit_to_celsius(v)
        return v
    s = str(raw).strip()
    if not s:
        return None
    m = re.match(r"^([\d.]+)\s*°?\s*([FC])?$", s, re.I)
    if m:
        v = float(m.group(1))
        u = (m.group(2) or "C").upper()
        return fahrenheit_to_celsius(v) if u == "F" else v
    try:
        return float(s)
    except ValueError:
        return None


def parse_iso_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def weather_cell_id_for(*, source: str, grid_id: str | None, lat: float, lon: float, valid_time: datetime) -> str:
    gid = grid_id or "unknown"
    vt = valid_time.astimezone(timezone.utc).strftime("%Y%m%dT%H%MZ")
    return f"{source}:{gid}:{lat:.5f}:{lon:.5f}:{vt}"


def normalized_cell_from_dict(
    cell: dict[str, Any],
    *,
    source: str,
    grid_id: str | None,
    valid_time: datetime,
    forecast_time: datetime | None = None,
) -> dict[str, Any]:
    """
    Map a cell dict (fixture or adapter) to normalized.weather_grid_cells columns.

    Expected keys (flexible): latitude/longitude or lat/lon;
    wind_speed_mps or windSpeed; wind_direction_degrees or windDirection (degrees FROM);
    temperature_c or temperature + temperature_unit; relative_humidity_percent or relativeHumidity.
    """

    lat = cell.get("latitude", cell.get("lat"))
    lon = cell.get("longitude", cell.get("lon"))
    if lat is None or lon is None:
        raise ValueError("grid cell missing latitude/longitude")
    lat_f = float(lat)
    lon_f = float(lon)

    ws = cell.get("wind_speed_mps")
    if ws is None and cell.get("windSpeed") is not None:
        ws = parse_wind_speed_to_mps(cell.get("windSpeed"))
    elif ws is not None:
        ws = float(ws)

    wd = cell.get("wind_direction_degrees")
    if wd is None and cell.get("windDirection") is not None:
        wd = cell.get("windDirection")
    wd_f = float(wd) if wd is not None else None

    tc = cell.get("temperature_c")
    if tc is None:
        tc = parse_temperature_to_celsius(
            cell.get("temperature"),
            unit_hint=str(cell.get("temperature_unit") or cell.get("temperatureUnit") or ""),
        )
    else:
        tc = float(tc)

    rh = cell.get("relative_humidity_percent")
    if rh is None:
        rh = cell.get("relativeHumidity")
    rh_f = float(rh) if rh is not None else None

    wcid = weather_cell_id_for(source=source, grid_id=grid_id, lat=lat_f, lon=lon_f, valid_time=valid_time)

    return {
        "weather_cell_id": wcid,
        "source": source,
        "grid_id": grid_id,
        "valid_time": valid_time,
        "forecast_time": forecast_time,
        "latitude": lat_f,
        "longitude": lon_f,
        "wind_speed_mps": ws,
        "wind_direction_degrees": wd_f,
        "temperature_c": tc,
        "relative_humidity_percent": rh_f,
    }


def cells_from_envelope_record(record: dict[str, Any], *, source: str, grid_id: str | None, valid_time: datetime) -> list[dict[str, Any]]:
    cells_raw = record.get("cells")
    if not isinstance(cells_raw, list):
        raise ValueError("record.cells must be a list")
    out: list[dict[str, Any]] = []
    for i, c in enumerate(cells_raw):
        if not isinstance(c, dict):
            raise ValueError(f"record.cells[{i}] must be an object")
        ft = parse_iso_datetime(c.get("forecast_time") or c.get("forecastTime"))
        out.append(normalized_cell_from_dict(c, source=source, grid_id=grid_id, valid_time=valid_time, forecast_time=ft))
    return out
