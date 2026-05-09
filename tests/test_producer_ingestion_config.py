from __future__ import annotations

import json

from wildfire_smoke.producers import openaq_producer
from wildfire_smoke.settings import Settings, load_yaml_config


def test_openaq_ingestion_config_never_embeds_api_key(monkeypatch) -> None:
    monkeypatch.setenv("OPENAQ_API_KEY", "super-secret-not-for-config-json")
    settings = Settings.from_env()
    cfg = load_yaml_config("sources.yaml")["openaq"]

    payload = openaq_producer._openaq_ingestion_config(settings, cfg)

    dumped = json.dumps(payload)
    assert "super-secret-not-for-config-json" not in dumped
    assert "OPENAQ_API_KEY" not in dumped
