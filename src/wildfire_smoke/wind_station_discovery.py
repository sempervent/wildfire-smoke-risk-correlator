"""Bounded NWS observation station discovery using WIND_BBOX (operational lag stack)."""

from __future__ import annotations

import json
import logging
import math
import os
from pathlib import Path

import httpx

from wildfire_smoke.live_bbox import BBox, bbox_allowed_for_live_ingest, parse_bbox
from wildfire_smoke.settings import repo_root

log = logging.getLogger(__name__)

NWS_BASE = "https://api.weather.gov"

DEFAULT_USER_AGENT = (
    "(wildfire-smoke-risk-correlator, github.com/wildfire-smoke-risk-correlator)"
)


def wind_discovery_limit() -> int:
    raw = os.environ.get("WIND_STATION_DISCOVERY_LIMIT", "25")
    return max(1, int(str(raw).strip()))


def wind_discovery_radius_km() -> float | None:
    raw = os.environ.get("WIND_STATION_DISCOVERY_RADIUS_KM", "").strip()
    if not raw:
        return None
    return max(0.0, float(raw))


def nws_user_agent() -> str:
    return os.environ.get("NWS_USER_AGENT", DEFAULT_USER_AGENT).strip() or DEFAULT_USER_AGENT


def warn_if_default_user_agent_live() -> None:
    ua = nws_user_agent()
    if ua == DEFAULT_USER_AGENT or "wildfire-smoke-risk-correlator" in ua:
        log.warning(
            "nws_user_agent_default",
            extra={
                "hint": "Set NWS_USER_AGENT to a contact URL or email per weather.gov API guidelines.",
            },
        )


def parse_wind_bbox(raw: str) -> BBox:
    return parse_bbox(raw)


def assert_wind_bbox_allowed_for_discovery(bbox: BBox) -> None:
    """Reuse LIVE_INGEST span guards for WIND_BBOX discovery (bounded ops)."""
    allow = os.environ.get("LIVE_INGEST_ALLOW_LARGE_BBOX", "0").strip().lower() in {"1", "true", "yes"}
    if allow:
        return
    if not bbox_allowed_for_live_ingest(bbox):
        lim = os.environ.get("LIVE_INGEST_MAX_SPAN_DEG", "14")
        raise ValueError(
            f"WIND_BBOX span too large (max lon/lat span must be <= {lim} degrees); "
            "narrow WIND_BBOX or set LIVE_INGEST_ALLOW_LARGE_BBOX=1."
        )


def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(min(1.0, a)))


def _point_near_bbox(lon: float, lat: float, bbox: BBox, radius_km: float | None) -> bool:
    inside = bbox.min_lon <= lon <= bbox.max_lon and bbox.min_lat <= lat <= bbox.max_lat
    if inside:
        return True
    if radius_km is None or radius_km <= 0:
        return False
    corners = [
        (bbox.min_lon, bbox.min_lat),
        (bbox.max_lon, bbox.min_lat),
        (bbox.min_lon, bbox.max_lat),
        (bbox.max_lon, bbox.max_lat),
    ]
    center_lon = (bbox.min_lon + bbox.max_lon) / 2.0
    center_lat = (bbox.min_lat + bbox.max_lat) / 2.0
    corners.append((center_lon, center_lat))
    return any(_haversine_km(lon, lat, clon, clat) <= radius_km for clon, clat in corners)


def station_ids_from_fixture(path: Path, bbox: BBox, *, limit: int, radius_km: float | None) -> list[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    feats = data.get("features") if isinstance(data, dict) else None
    if not isinstance(feats, list):
        raise ValueError("stations fixture must be a FeatureCollection with features[]")
    out: list[str] = []
    for feat in feats:
        if not isinstance(feat, dict):
            continue
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates")
        props = feat.get("properties") or {}
        sid = props.get("stationIdentifier") or props.get("station_id")
        if not coords or len(coords) < 2 or not sid:
            continue
        lon, lat = float(coords[0]), float(coords[1])
        if _point_near_bbox(lon, lat, bbox, radius_km):
            out.append(str(sid).strip().upper())
        if len(out) >= limit:
            break
    return out[:limit]


def station_ids_from_nws_api(
    client: httpx.Client,
    bbox: BBox,
    *,
    limit: int,
    radius_km: float | None,
    page_limit: int = 500,
) -> list[str]:
    """Paginate /stations and filter by bbox (+ optional radius buffer)."""

    warn_if_default_user_agent_live()
    headers = {"User-Agent": nws_user_agent(), "Accept": "application/geo+json"}
    url: str | None = f"{NWS_BASE}/stations?limit={page_limit}"
    seen: set[str] = set()
    candidates: list[str] = []

    while url and len(candidates) < limit:
        resp = client.get(url, headers=headers, timeout=60.0)
        resp.raise_for_status()
        payload = resp.json()
        feats = payload.get("features") if isinstance(payload, dict) else None
        if not isinstance(feats, list):
            break
        for feat in feats:
            if not isinstance(feat, dict):
                continue
            geom = feat.get("geometry") or {}
            coords = geom.get("coordinates")
            props = feat.get("properties") or {}
            sid = props.get("stationIdentifier")
            if not coords or len(coords) < 2 or not sid:
                continue
            lon, lat = float(coords[0]), float(coords[1])
            if _point_near_bbox(lon, lat, bbox, radius_km):
                u = str(sid).strip().upper()
                if u not in seen:
                    seen.add(u)
                    candidates.append(u)
            if len(candidates) >= limit:
                break
        pagination = payload.get("pagination") if isinstance(payload, dict) else None
        next_url = None
        if isinstance(pagination, dict):
            next_url = pagination.get("next")
        url = str(next_url) if next_url else None

    return candidates[:limit]


def resolve_wind_station_ids_for_live(
    *,
    wind_station_ids: tuple[str, ...],
    wind_bbox_raw: str | None,
    fixture_path: Path | None = None,
) -> list[str]:
    """
    WIND_STATION_IDS wins when non-empty.
    Otherwise discover from WIND_BBOX using fixture (tests) or NWS API (live).
    """

    if wind_station_ids:
        return list(wind_station_ids)

    if not wind_bbox_raw or not str(wind_bbox_raw).strip():
        return []

    bbox = parse_wind_bbox(str(wind_bbox_raw).strip())
    assert_wind_bbox_allowed_for_discovery(bbox)
    limit = wind_discovery_limit()
    radius_km = wind_discovery_radius_km()

    fp_raw = os.environ.get("NWS_STATIONS_FIXTURE_JSON", "").strip()
    if fp_raw:
        path = Path(fp_raw) if Path(fp_raw).is_absolute() else repo_root() / fp_raw
        return station_ids_from_fixture(path, bbox, limit=limit, radius_km=radius_km)

    if fixture_path is not None:
        return station_ids_from_fixture(fixture_path, bbox, limit=limit, radius_km=radius_km)

    with httpx.Client() as client:
        return station_ids_from_nws_api(client, bbox, limit=limit, radius_km=radius_km)
