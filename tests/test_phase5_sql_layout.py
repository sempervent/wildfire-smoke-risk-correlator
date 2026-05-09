from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_phase5_migration_defines_attempts_and_ops_views() -> None:
    mig = repo_root() / "sql/migrations/004_phase5_notification_reliability.sql"
    init = repo_root() / "docker/postgres/initdb/60_phase5.sql"
    assert mig.is_file()
    assert init.is_file()
    txt = mig.read_text()
    assert "analytics.notification_attempts" in txt
    assert "analytics.operational_runs" in txt
    assert "v_open_alert_events" in txt
    assert "v_notification_attempt_summary" in txt
    assert "v_notification_failures" in txt
    assert "v_alert_delivery_state" in txt
    assert "v_recent_operational_cycles" in txt
