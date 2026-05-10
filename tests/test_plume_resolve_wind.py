from __future__ import annotations

from types import SimpleNamespace

from wildfire_smoke.spark import compute_plume_exposure as pe


def test_resolve_wind_wind_v1_uses_station_path(monkeypatch) -> None:
    calls: list[str] = []

    def _station(*a, **k):
        calls.append("station")
        return {"kind": "station", "wind_observation_id": "w1"}

    monkeypatch.setattr(pe, "_fetch_station_wind", _station)
    monkeypatch.setattr(pe, "_fetch_grid_wind", lambda *a, **k: (_ for _ in ()).throw(AssertionError("grid should not run")))

    settings = SimpleNamespace()
    wind, fb = pe._resolve_wind(None, settings, {}, plume_model_version="wind_v1")
    assert wind["kind"] == "station"
    assert fb is False
    assert calls == ["station"]


def test_resolve_wind_grid_prefers_grid(monkeypatch) -> None:
    grid_payload = {
        "kind": "grid",
        "weather_cell_id": "cell-1",
        "wind_direction_degrees": 200.0,
        "wind_speed_mps": 5.0,
        "dist_km": 12.0,
        "time_delta_minutes": 30.0,
        "match_method": "nearest_grid_cell",
    }

    monkeypatch.setattr(pe, "_fetch_grid_wind", lambda *a, **k: grid_payload)
    monkeypatch.setattr(pe, "_fetch_station_wind", lambda *a, **k: (_ for _ in ()).throw(AssertionError("station should not run")))

    settings = SimpleNamespace(plume_grid_fallback_to_station=False, fire_weather_match_method="nearest_grid_cell")
    wind, fb = pe._resolve_wind(None, settings, {}, plume_model_version="wind_grid_v2")
    assert wind == grid_payload
    assert fb is False


def test_resolve_wind_grid_fallback_to_station(monkeypatch) -> None:
    station_payload = {
        "kind": "station",
        "wind_observation_id": "w9",
        "weather_cell_id": None,
        "wind_direction_degrees": 210.0,
        "wind_speed_mps": 3.0,
        "dist_km": 5.0,
        "time_delta_minutes": None,
        "match_method": None,
    }

    monkeypatch.setattr(pe, "_fetch_grid_wind", lambda *a, **k: None)
    monkeypatch.setattr(pe, "_fetch_station_wind", lambda *a, **k: station_payload)

    settings = SimpleNamespace(plume_grid_fallback_to_station=True, fire_weather_match_method="nearest_grid_cell")
    wind, fb = pe._resolve_wind(None, settings, {}, plume_model_version="wind_grid_v2")
    assert wind == station_payload
    assert fb is True


def test_resolve_wind_grid_no_match_no_fallback(monkeypatch) -> None:
    monkeypatch.setattr(pe, "_fetch_grid_wind", lambda *a, **k: None)

    def _no_station(*a, **k):
        raise AssertionError("station must not be queried when fallback is off")

    monkeypatch.setattr(pe, "_fetch_station_wind", _no_station)
    settings = SimpleNamespace(plume_grid_fallback_to_station=False, fire_weather_match_method="nearest_grid_cell")
    wind, fb = pe._resolve_wind(None, settings, {}, plume_model_version="wind_grid_v2")
    assert wind is None
    assert fb is False
