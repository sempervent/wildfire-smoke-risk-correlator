from __future__ import annotations

from wildfire_smoke.census_config import (
    load_census_yaml,
    load_national_counties_full_us,
    resolved_state_fps,
    state_fps_sql_in_clause,
    validation_thresholds,
)


def test_resolved_state_fps_default_yaml(monkeypatch) -> None:
    monkeypatch.delenv("CENSUS_STATEFPS", raising=False)
    monkeypatch.delenv("CENSUS_STATEFP", raising=False)
    y = load_census_yaml()
    assert resolved_state_fps(y) == ["47"]


def test_resolved_state_fps_env_multi(monkeypatch) -> None:
    monkeypatch.setenv("CENSUS_STATEFPS", "47, 37")
    y = load_census_yaml()
    assert resolved_state_fps(y) == ["47", "37"]


def test_resolved_state_fps_env_single_pad(monkeypatch) -> None:
    monkeypatch.setenv("CENSUS_STATEFP", "7")
    y = load_census_yaml()
    assert resolved_state_fps(y) == ["07"]


def test_state_fps_sql_in_clause() -> None:
    assert state_fps_sql_in_clause(["47", "37"]) == "'47','37'"


def test_validation_multi_state_threshold(monkeypatch) -> None:
    monkeypatch.delenv("CENSUS_LOAD_NATIONAL_COUNTIES", raising=False)
    y = load_census_yaml()
    t = validation_thresholds(y, num_states=2)
    assert t.min_total_counties >= 90
    assert t.min_total_tracts >= 1000


def test_load_national_counties_env(monkeypatch) -> None:
    monkeypatch.delenv("CENSUS_LOAD_NATIONAL_COUNTIES", raising=False)
    assert load_national_counties_full_us() is False
    monkeypatch.setenv("CENSUS_LOAD_NATIONAL_COUNTIES", "1")
    assert load_national_counties_full_us() is True


def test_yaml_states_list(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("CENSUS_STATEFPS", raising=False)
    monkeypatch.delenv("CENSUS_STATEFP", raising=False)
    p = tmp_path / "census.yaml"
    p.write_text(
        "state:\n  statefp: '99'\n"
        "states:\n  - statefp: '37'\n    name: NC\n"
        "  - statefp: '47'\n    name: TN\n"
        "years_try: [2024]\n"
        "validation:\n  min_counties: 1\n  min_tracts: 1\n",
        encoding="utf-8",
    )
    y = load_census_yaml(p)
    assert resolved_state_fps(y) == ["37", "47"]
