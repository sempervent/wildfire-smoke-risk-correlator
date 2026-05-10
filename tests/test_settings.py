from __future__ import annotations

import pytest

from wildfire_smoke.settings import Settings, kafka_topics


def test_kafka_topics_include_expected_keys() -> None:
    topics = kafka_topics()
    assert topics["firms_raw_topic"] == "firms.hotspots.raw"
    assert topics["openaq_raw_topic"] == "openaq.measurements.raw"
    assert topics["smoke_risk_topic"] == "smoke.risk.scores"
    assert topics["firms_dlq_topic"] == "firms.hotspots.dlq"
    assert topics["openaq_dlq_topic"] == "openaq.measurements.dlq"
    assert topics["wind_dlq_topic"] == "weather.wind.dlq"
    assert topics["grid_weather_raw_topic"] == "weather.grid.raw"
    assert topics["grid_weather_dlq_topic"] == "weather.grid.dlq"
    assert topics["grid_weather_normalized_topic"] == "weather.grid.normalized"
    assert topics["normalization_errors_topic"] == "normalization.errors"


def test_settings_env_overrides(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_DB", "custom_db")
    monkeypatch.setenv("FIRMS_SOURCE", "TEST_SOURCE")
    monkeypatch.setenv("FIRMS_DRY_RUN", "true")

    s = Settings.from_env()
    assert s.postgres_db == "custom_db"
    assert s.firms_source == "TEST_SOURCE"
    assert s.firms_dry_run is True

    # Secrets must exist as attributes but must never be required for dry-run FIRMS path.
    monkeypatch.delenv("FIRMS_MAP_KEY", raising=False)
    s2 = Settings.from_env()
    assert s2.firms_map_key is None


def test_smoke_risk_settings_defaults(monkeypatch) -> None:
    monkeypatch.delenv("SMOKE_RISK_MODEL_VERSION", raising=False)
    monkeypatch.delenv("SMOKE_RISK_LOOKBACK_HOURS", raising=False)
    monkeypatch.delenv("SMOKE_RISK_NEARBY_KM", raising=False)
    monkeypatch.delenv("SMOKE_RISK_GEOGRAPHIES", raising=False)

    s = Settings.from_env()
    assert s.smoke_risk_model_version == "v2"
    assert s.smoke_risk_lookback_hours == 24
    assert s.smoke_risk_nearby_km == 50.0
    assert s.smoke_risk_geographies == "both"


def test_smoke_risk_settings_invalid_model_version(monkeypatch) -> None:
    monkeypatch.setenv("SMOKE_RISK_MODEL_VERSION", "v999")
    with pytest.raises(ValueError, match="SMOKE_RISK_MODEL_VERSION"):
        Settings.from_env()


def test_grid_weather_points_parsing(monkeypatch) -> None:
    monkeypatch.setenv("GRID_WEATHER_POINTS", "-86.8,36.16;-86.7,36.2")
    s = Settings.from_env()
    assert s.grid_weather_points_lonlat == ((-86.8, 36.16), (-86.7, 36.2))


def test_grid_weather_points_invalid(monkeypatch) -> None:
    monkeypatch.setenv("GRID_WEATHER_POINTS", "lonlat")
    with pytest.raises(ValueError, match="GRID_WEATHER_POINTS"):
        Settings.from_env()


def test_fixture_time_mode_invalid(monkeypatch) -> None:
    monkeypatch.setenv("FIXTURE_TIME_MODE", "nope")
    with pytest.raises(ValueError, match="FIXTURE_TIME_MODE"):
        Settings.from_env()


def test_smoke_risk_geographies_override(monkeypatch) -> None:
    monkeypatch.setenv("SMOKE_RISK_GEOGRAPHIES", "county")
    s = Settings.from_env()
    assert s.smoke_risk_geographies == "county"


def test_smoke_risk_model_version_v5(monkeypatch) -> None:
    monkeypatch.setenv("SMOKE_RISK_MODEL_VERSION", "v5")
    s = Settings.from_env()
    assert s.smoke_risk_model_version == "v5"


def test_dispersion_defaults(monkeypatch) -> None:
    monkeypatch.delenv("DISPERSION_ENABLED", raising=False)
    monkeypatch.delenv("DISPERSION_MODEL_VERSION", raising=False)
    s = Settings.from_env()
    assert s.dispersion_enabled is False
    assert s.dispersion_model_version == "gaussian_v0"
    assert s.dispersion_max_target_geographies >= 1


def test_calibration_defaults(monkeypatch) -> None:
    monkeypatch.delenv("CALIBRATION_MIN_AQ_OBSERVATIONS", raising=False)
    monkeypatch.delenv("RISK_EVAL_MIN_MATCH_COUNT", raising=False)
    s = Settings.from_env()
    assert s.calibration_min_aq_observations == 3
    assert s.calibration_high_pm25_threshold == 35.0
    assert s.calibration_low_pm25_threshold == 12.0
    assert "0-3" in s.calibration_lag_windows_hours
    assert s.risk_eval_min_match_count == 3


def test_settings_requires_key_for_live_firms(monkeypatch) -> None:
    monkeypatch.delenv("FIRMS_MAP_KEY", raising=False)
    monkeypatch.setenv("FIRMS_DRY_RUN", "0")

    from wildfire_smoke.producers import firms_producer as fp

    # firms_producer.main should fail fast without a map key.
    try:
        fp.main()
    except RuntimeError as exc:
        assert "FIRMS_MAP_KEY" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
