from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_dlq_migrations_initdb_views_and_alerts_exist() -> None:
    root = repo_root()
    m7 = root / "sql/migrations/007_phase7_dlq_and_offsets.sql"
    i72 = root / "docker/postgres/initdb/72_phase7_dlq_offsets.sql"
    vdlq = root / "sql/views/zzz_phase7_dlq_views.sql"
    val = root / "sql/views/zzz_phase7_fn_alert_candidates.sql"
    for p in (m7, i72, vdlq, val):
        assert p.is_file(), f"missing {p}"

    ddl = m7.read_text()
    assert "analytics.parse_errors" in ddl
    assert "analytics.kafka_consumer_offsets" in ddl

    views_txt = vdlq.read_text()
    assert "v_parse_errors_open" in views_txt
    assert "v_dlq_operational_summary" in views_txt

    alerts_txt = val.read_text()
    assert "p_parse_errors_warn_count" in alerts_txt
    assert "parse_errors_high" in alerts_txt
    assert "consumer_offset_stale" in alerts_txt
