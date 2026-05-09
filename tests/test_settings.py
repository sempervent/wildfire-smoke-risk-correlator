from __future__ import annotations

import pytest

from wildfire_smoke.settings import Settings, kafka_topics


def test_kafka_topics_include_expected_keys() -> None:
    topics = kafka_topics()
    assert topics["firms_raw_topic"] == "firms.hotspots.raw"
    assert topics["openaq_raw_topic"] == "openaq.measurements.raw"
    assert topics["smoke_risk_topic"] == "smoke.risk.scores"


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


def test_smoke_risk_geographies_override(monkeypatch) -> None:
    monkeypatch.setenv("SMOKE_RISK_GEOGRAPHIES", "county")
    s = Settings.from_env()
    assert s.smoke_risk_geographies == "county"


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
