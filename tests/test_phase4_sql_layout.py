from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_phase4_migration_defines_alert_events() -> None:
    mig = repo_root() / "sql/migrations/003_phase4_alerts.sql"
    init = repo_root() / "docker/postgres/initdb/50_phase4.sql"
    assert mig.is_file()
    assert init.is_file()
    body = mig.read_text()
    assert "analytics.alert_events" in body
    assert "alert_events_open_fingerprint_uidx" in body
