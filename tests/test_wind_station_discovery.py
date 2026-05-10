from __future__ import annotations

import pytest

from wildfire_smoke.settings import repo_root
from wildfire_smoke.wind_station_discovery import (
    assert_wind_bbox_allowed_for_discovery,
    resolve_wind_station_ids_for_live,
    station_ids_from_fixture,
    station_ids_from_nws_api,
)


def test_station_ids_from_fixture_respects_bbox_and_limit() -> None:
    fixture = repo_root() / "tests/fixtures/nws_stations_sample.json"
    bbox_raw = "-87.0,35.5,-82.0,36.8"
    from wildfire_smoke.wind_station_discovery import parse_wind_bbox

    bbox = parse_wind_bbox(bbox_raw)
    ids = station_ids_from_fixture(fixture, bbox, limit=10, radius_km=None)
    assert "KBNA" in ids
    assert "KTYS" in ids
    assert "KLAX" not in ids

    one = station_ids_from_fixture(fixture, bbox, limit=1, radius_km=None)
    assert len(one) == 1


def test_wind_station_ids_override_wins() -> None:
    ids = resolve_wind_station_ids_for_live(
        wind_station_ids=("KFOO", "KBAR"),
        wind_bbox_raw="-87,35,-82,37",
        fixture_path=None,
    )
    assert ids == ["KFOO", "KBAR"]


def test_resolve_uses_fixture_env(monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = repo_root() / "tests/fixtures/nws_stations_sample.json"
    monkeypatch.setenv("NWS_STATIONS_FIXTURE_JSON", str(fixture))
    monkeypatch.delenv("WIND_STATION_IDS", raising=False)
    ids = resolve_wind_station_ids_for_live(wind_station_ids=(), wind_bbox_raw="-87,35.5,-82,36.8")
    assert "KBNA" in ids


def test_bbox_span_guard_requires_allow_large(monkeypatch: pytest.MonkeyPatch) -> None:
    from wildfire_smoke.wind_station_discovery import parse_wind_bbox

    monkeypatch.delenv("LIVE_INGEST_ALLOW_LARGE_BBOX", raising=False)
    huge = parse_wind_bbox("-125,24,-66,50")
    with pytest.raises(ValueError, match="WIND_BBOX span too large"):
        assert_wind_bbox_allowed_for_discovery(huge)

    monkeypatch.setenv("LIVE_INGEST_ALLOW_LARGE_BBOX", "1")
    assert_wind_bbox_allowed_for_discovery(huge)


def test_station_ids_from_nws_api_parsed_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Uses a stub client; no real HTTP."""

    class Resp:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [-86.7, 36.1]},
                        "properties": {"stationIdentifier": "KNQA"},
                    }
                ],
                "pagination": {},
            }

    class Client:
        def get(self, url: str, headers: dict | None = None, timeout: float = 0) -> Resp:
            return Resp()

    monkeypatch.setenv("WIND_STATION_DISCOVERY_LIMIT", "5")
    from wildfire_smoke.wind_station_discovery import parse_wind_bbox

    bbox = parse_wind_bbox("-87.2,35.9,-86.5,36.3")
    ids = station_ids_from_nws_api(Client(), bbox, limit=5, radius_km=None)
    assert ids == ["KNQA"]
