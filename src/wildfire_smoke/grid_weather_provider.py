"""Gridded weather providers (fixture + NWS gridpoint)."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

import httpx

from wildfire_smoke.fixture_time import compute_shift_to_anchor, rewrite_grid_weather_dict
from wildfire_smoke.grid_weather_records import parse_iso_datetime, parse_wind_speed_to_mps
from wildfire_smoke.live_bbox import BBox, bbox_allowed_for_live_ingest, parse_bbox
from wildfire_smoke.settings import Settings, repo_root
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


def wind_direction_text_to_degrees(raw: Any) -> float | None:
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


@dataclass(frozen=True)
class GridWeatherBatch:
    grid_id: str | None
    valid_time: datetime
    cells: list[dict[str, Any]]
    envelope_metadata: dict[str, Any]


class GridWeatherProvider(Protocol):
    def fetch_batch(self) -> GridWeatherBatch:
        ...


def _assert_grid_bbox(bbox: BBox, settings: Settings) -> None:
    import os

    allow_large = os.environ.get("LIVE_INGEST_ALLOW_LARGE_BBOX", "0").strip().lower() in {"1", "true", "yes"}
    if allow_large:
        return
    if settings.grid_weather_refuse_large_bbox and not bbox_allowed_for_live_ingest(bbox):
        lim = os.environ.get("LIVE_INGEST_MAX_SPAN_DEG", "14")
        raise ValueError(
            f"GRID_WEATHER_BBOX span too large (max lon/lat span must be <= {lim} degrees); "
            "narrow bbox or set LIVE_INGEST_ALLOW_LARGE_BBOX=1."
        )


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


def _sanitized_points_url(lat: float, lon: float) -> str:
    return f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"


def _first_series_value(series: dict[str, Any] | None) -> tuple[Any, str | None]:
    """Return (value, validTime raw string) from first grid layer sample."""

    if not series or not isinstance(series, dict):
        return None, None
    vals = series.get("values")
    if not isinstance(vals, list) or not vals:
        return None, None
    first = vals[0]
    if not isinstance(first, dict):
        return None, None
    return first.get("value"), first.get("validTime")


def _split_interval_start(valid_time_raw: str | None) -> str | None:
    if not valid_time_raw:
        return None
    return str(valid_time_raw).split("/")[0].strip()


def _parse_grid_temperature_c(raw: Any, uom: str | None) -> float | None:
    if raw is None:
        return None
    v = float(raw)
    u = (uom or "").lower()
    if "degf" in u or "f" in u:
        return (v - 32.0) * 5.0 / 9.0
    return v


class FixtureGridWeatherProvider:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def fetch_batch(self) -> GridWeatherBatch:
        path = self._settings.grid_weather_fixture_json
        if not path.is_absolute():
            path = repo_root() / path
        data = json.loads(path.read_text(encoding="utf-8"))
        meta: dict[str, Any] = {}
        if self._settings.fixture_time_mode == "relative":
            vt0 = parse_iso_datetime(data.get("valid_time"))
            if vt0 is not None:
                shift = compute_shift_to_anchor(
                    [vt0],
                    base_hours_ago=self._settings.fixture_relative_base_hours_ago,
                )
                if shift.total_seconds() != 0:
                    orig = rewrite_grid_weather_dict(data, shift)
                    meta["original_observed_at"] = orig
                    meta["fixture_time_rewritten"] = True

        grid_id = str(data["grid_id"]) if data.get("grid_id") else None
        vt = parse_iso_datetime(data.get("valid_time"))
        if vt is None:
            vt = datetime.now(timezone.utc).replace(microsecond=0)
        cells_raw = data.get("cells")
        if not isinstance(cells_raw, list):
            raise ValueError("fixture must contain cells array")
        cells = [c for c in cells_raw if isinstance(c, dict)]
        cells = cells[: self._settings.grid_weather_max_points]
        return GridWeatherBatch(grid_id=grid_id, valid_time=vt, cells=cells, envelope_metadata=meta)


class NwsGridpointWeatherProvider:
    """Bounded live fetch via /points and forecastGridData."""

    def __init__(self, settings: Settings, bbox: BBox) -> None:
        self._settings = settings
        self._bbox = bbox
        self._point_cache: dict[tuple[int, int], dict[str, Any]] = {}

    def _points_properties(self, client: httpx.Client, lat: float, lon: float) -> dict[str, Any]:
        key = (round(lat, 4), round(lon, 4))
        if key in self._point_cache:
            return self._point_cache[key]
        url = _sanitized_points_url(lat, lon)
        log.info("nws_grid_points_fetch", extra={"url": url})
        resp = client.get(url, headers={"User-Agent": nws_user_agent(), "Accept": "application/geo+json"})
        if resp.status_code >= 400:
            raise RuntimeError(f"NWS points request failed status={resp.status_code} url={url}")
        payload = resp.json()
        props = payload.get("properties") or {}
        self._point_cache[key] = props
        return props

    def _fetch_grid_cell(
        self,
        client: httpx.Client,
        lat: float,
        lon: float,
    ) -> tuple[str | None, datetime, dict[str, Any]]:
        props = self._points_properties(client, lat, lon)
        grid_id = str(props.get("gridId") or props.get("grid") or "nws-live")
        grid_url = props.get("forecastGridData")
        if not grid_url:
            raise RuntimeError("NWS points response missing forecastGridData URL")
        safe_log = str(grid_url).split("?")[0]
        log.info("nws_forecast_griddata_fetch", extra={"url": safe_log})
        gr = client.get(str(grid_url), headers={"User-Agent": nws_user_agent(), "Accept": "application/geo+json"})
        if gr.status_code >= 400:
            raise RuntimeError(f"NWS forecastGridData failed status={gr.status_code}")
        gj = gr.json()
        gprops = gj.get("properties") or {}

        ws_raw, ws_time = _first_series_value(gprops.get("windSpeed"))
        wd_raw, _ = _first_series_value(gprops.get("windDirection"))
        temp_raw, temp_time = _first_series_value(gprops.get("temperature"))
        rh_raw, _ = _first_series_value(gprops.get("relativeHumidity"))

        temp_uom = None
        tseries = gprops.get("temperature")
        if isinstance(tseries, dict):
            temp_uom = str(tseries.get("uom") or "")

        ws_mps = parse_wind_speed_to_mps(ws_raw) if ws_raw is not None else None
        wd_deg = wind_direction_text_to_degrees(wd_raw)
        temp_c = _parse_grid_temperature_c(temp_raw, temp_uom)
        rh = float(rh_raw) if rh_raw is not None else None

        vt_raw = _split_interval_start(ws_time) or _split_interval_start(temp_time)
        valid_time = parse_iso_datetime(vt_raw) if vt_raw else datetime.now(timezone.utc).replace(microsecond=0)
        if isinstance(valid_time, datetime) and valid_time.tzinfo is None:
            valid_time = valid_time.replace(tzinfo=timezone.utc)

        cell = {
            "latitude": lat,
            "longitude": lon,
            "wind_speed_mps": ws_mps,
            "wind_direction_degrees": wd_deg,
            "temperature_c": temp_c,
            "relative_humidity_percent": rh,
        }
        return grid_id, valid_time, cell

    def fetch_batch(self) -> GridWeatherBatch:
        warn_if_default_user_agent_live()
        _assert_grid_bbox(self._bbox, self._settings)

        pts: list[tuple[float, float]] = []
        if self._settings.grid_weather_points_lonlat:
            for lon, lat in self._settings.grid_weather_points_lonlat[: self._settings.grid_weather_max_points]:
                pts.append((lat, lon))
        else:
            pts = _generate_grid_points(
                self._bbox,
                cell_step=self._settings.grid_weather_cell_size_degrees,
                max_points=self._settings.grid_weather_max_points,
            )

        if not pts:
            raise RuntimeError("no grid weather points resolved (check GRID_WEATHER_BBOX / GRID_WEATHER_POINTS)")

        cells: list[dict[str, Any]] = []
        grid_id: str | None = None
        valid_time: datetime | None = None

        with httpx.Client(timeout=60.0) as client:
            for lat, lon in pts:
                gid, vt, cell = self._fetch_grid_cell(client, lat, lon)
                grid_id = gid or grid_id
                valid_time = vt if valid_time is None else valid_time
                cells.append(cell)

        vt_final = valid_time or datetime.now(timezone.utc).replace(microsecond=0)
        return GridWeatherBatch(grid_id=grid_id, valid_time=vt_final, cells=cells, envelope_metadata={})


def grid_weather_provider_for_settings(settings: Settings) -> GridWeatherProvider:
    bbox_raw = settings.grid_weather_bbox
    if not bbox_raw:
        raise ValueError("GRID_WEATHER_BBOX is required for grid weather producer")
    bbox = parse_bbox(bbox_raw)
    if settings.grid_weather_dry_run:
        return FixtureGridWeatherProvider(settings)
    if settings.grid_weather_source not in {"nws_gridpoint"}:
        raise ValueError(f"Unsupported GRID_WEATHER_SOURCE for live mode: {settings.grid_weather_source!r}")
    return NwsGridpointWeatherProvider(settings, bbox)


def attach_batch_metadata(envelope: dict[str, Any], batch: GridWeatherBatch) -> None:
    for k, v in batch.envelope_metadata.items():
        envelope[k] = v
