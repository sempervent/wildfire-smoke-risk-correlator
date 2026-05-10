from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_match_fire_job_orders_by_distance_then_time_delta() -> None:
    src = repo_root() / "src/wildfire_smoke/spark/match_fire_weather.py"
    text = src.read_text()
    assert "ST_DWithin" in text
    assert "normalized.weather_grid_cells" in text
    assert "ORDER BY" in text
    assert "ST_DistanceSphere" in text
    assert "abs(EXTRACT(EPOCH FROM (c.valid_time" in text
