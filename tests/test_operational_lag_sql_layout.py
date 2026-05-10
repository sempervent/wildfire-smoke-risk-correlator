from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_operational_lag_migrations_initdb_views_and_alert_fn_exist() -> None:
    root = repo_root()
    m8 = root / "sql/migrations/008_phase8_operational_lag.sql"
    i73 = root / "docker/postgres/initdb/73_phase8_operational_lag.sql"
    vops = root / "sql/views/zzz_phase8_operational_views.sql"
    val = root / "sql/views/zzz_phase8_fn_alert_candidates.sql"
    for p in (m8, i73, vops, val):
        assert p.is_file(), f"missing {p}"

    ddl = m8.read_text()
    assert "analytics.kafka_topic_offsets" in ddl
    assert "analytics.kafka_consumer_lag_observations" in ddl
    assert "analytics.dlq_replay_runs" in ddl
    assert "analytics.dlq_replay_items" in ddl

    views_txt = vops.read_text()
    assert "v_kafka_topic_depth" in views_txt
    assert "v_consumer_lag_latest" in views_txt
    assert "v_dlq_topic_depth" in views_txt
    assert "v_pipeline_lag_summary" in views_txt

    alerts_txt = val.read_text()
    assert "p_parser_spike_warn_count" in alerts_txt
    assert "kafka_lag_high" in alerts_txt
    assert "dlq_depth_high" in alerts_txt
    assert "replay_failures_recent" in alerts_txt
