from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytest

from wildfire_smoke.grid_weather_provider import (
    FixtureGridWeatherProvider,
    NwsGridpointWeatherProvider,
    grid_weather_provider_for_settings,
    wind_direction_text_to_degrees,
)
from wildfire_smoke.live_bbox import parse_bbox
from wildfire_smoke.settings import Settings


def test_wind_direction_cardinal() -> None:
    assert wind_direction_text_to_degrees("NW") == pytest.approx(315.0)


def test_grid_weather_provider_fixture_dispatch(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("GRID_WEATHER_DRY_RUN", "1")
    monkeypatch.setenv("GRID_WEATHER_BBOX", "-88.2,34.9,-81.6,36.7")
    monkeypatch.setenv("FIXTURE_TIME_MODE", "static")
    p = tmp_path / "gw.json"
    p.write_text(
        '{"grid_id":"t","valid_time":"2026-05-09T18:00:00+00:00","cells":[{"latitude":36,"longitude":-86,"wind_direction_degrees":200}]}'
    )
    monkeypatch.setenv("GRID_WEATHER_FIXTURE_JSON", str(p))
    s = Settings.from_env()
    prov = grid_weather_provider_for_settings(s)
    assert isinstance(prov, FixtureGridWeatherProvider)
    batch = prov.fetch_batch()
    assert batch.cells and batch.cells[0]["latitude"] == 36.0


def test_nws_provider_fetches_griddata(monkeypatch) -> None:
    monkeypatch.setenv("GRID_WEATHER_DRY_RUN", "0")
    monkeypatch.setenv("GRID_WEATHER_BBOX", "-86.9,36.1,-86.6,36.2")
    monkeypatch.setenv("GRID_WEATHER_MAX_POINTS", "1")
    monkeypatch.setenv("NWS_USER_AGENT", "integration-test/1.0 (contact@test.example)")
    monkeypatch.setenv("GRID_WEATHER_POINTS", "")
    s = Settings.from_env()
    bbox = parse_bbox(s.grid_weather_bbox or "")
    prov = NwsGridpointWeatherProvider(s, bbox)

    points_payload = {
        "properties": {
            "gridId": "TST",
            "forecastGridData": "https://api.weather.gov/gridpoints/TST/1,2/forecast",
        }
    }
    grid_payload = {
        "properties": {
            "windSpeed": {"values": [{"validTime": "2026-05-09T18:00:00+00:00/PT1H", "value": 5}]},
            "windDirection": {"values": [{"validTime": "2026-05-09T18:00:00+00:00/PT1H", "value": 264}]},
            "temperature": {"uom": "wmoUnit:degC", "values": [{"validTime": "2026-05-09T18:00:00+00:00/PT1H", "value": 20}]},
            "relativeHumidity": {"values": [{"validTime": "2026-05-09T18:00:00+00:00/PT1H", "value": 45}]},
        }
    }

    class FakeResp:
        def __init__(self, payload: dict, status: int = 200):
            self._payload = payload
            self.status_code = status

        def json(self) -> dict:
            return self._payload

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise RuntimeError("bad status")

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url: str, **kwargs):
            if "/points/" in url:
                return FakeResp(points_payload)
            if "/forecast" in url:
                return FakeResp(grid_payload)
            raise AssertionError(f"unexpected url {url}")

    with patch("wildfire_smoke.grid_weather_provider.httpx.Client", FakeClient):
        batch = prov.fetch_batch()

    assert len(batch.cells) == 1
    c = batch.cells[0]
    assert c["wind_direction_degrees"] == pytest.approx(264.0)
    assert c["relative_humidity_percent"] == pytest.approx(45.0)
    assert isinstance(batch.valid_time, datetime)


def test_nws_provider_requires_user_agent_warning(monkeypatch, caplog) -> None:
    import logging

    monkeypatch.delenv("NWS_USER_AGENT", raising=False)
    monkeypatch.setenv("GRID_WEATHER_DRY_RUN", "0")
    monkeypatch.setenv("GRID_WEATHER_BBOX", "-86.9,36.1,-86.6,36.2")
    monkeypatch.setenv("GRID_WEATHER_MAX_POINTS", "1")
    s = Settings.from_env()
    bbox = parse_bbox(s.grid_weather_bbox or "")
    prov = NwsGridpointWeatherProvider(s, bbox)
    points_payload = {"properties": {"gridId": "X", "forecastGridData": "https://example.invalid/g"}}
    grid_payload = {
        "properties": {
            "windSpeed": {"values": [{"validTime": "2026-05-09T18:00:00+00:00/PT1H", "value": 1}]},
            "windDirection": {"values": [{"validTime": "2026-05-09T18:00:00+00:00/PT1H", "value": 180}]},
            "temperature": {"uom": "wmoUnit:degC", "values": [{"validTime": "2026-05-09T18:00:00+00:00/PT1H", "value": 10}]},
            "relativeHumidity": {"values": [{"validTime": "2026-05-09T18:00:00+00:00/PT1H", "value": 50}]},
        }
    }

    class FakeResp:
        def __init__(self, p):
            self._p = p
            self.status_code = 200

        def json(self):
            return self._p

        def raise_for_status(self):
            pass

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, **kwargs):
            if "/points/" in url:
                return FakeResp(points_payload)
            return FakeResp(grid_payload)

    caplog.set_level(logging.WARNING, logger="wildfire_smoke.wind_station_discovery")
    with patch("wildfire_smoke.grid_weather_provider.httpx.Client", FakeClient):
        prov.fetch_batch()
    assert any(getattr(r, "message", "") == "nws_user_agent_default" for r in caplog.records), caplog.text
