from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from wildfire_smoke.kafka_lag import collect_and_store_lag


def test_collect_and_store_lag_writes_topic_and_lag_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from wildfire_smoke import kafka_lag as kl

    class FakeConsumer:
        def __init__(self, *a: object, **kw: object) -> None:
            pass

        def poll(self, timeout_ms: int = 0) -> dict:
            return {}

        def partitions_for_topic(self, topic: str) -> set[int] | None:
            return {0} if topic == "firms.hotspots.raw" else None

        def end_offsets(self, tps: list) -> dict:
            out = {}
            for tp in tps:
                out[tp] = 100
            return out

        def close(self) -> None:
            return None

    monkeypatch.setattr(kl, "KafkaConsumer", FakeConsumer)
    monkeypatch.setattr(kl, "monitored_topics", lambda: ["firms.hotspots.raw"])

    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = [("spark-normalize-firms", "firms.hotspots.raw", 0, 90)]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    cm = MagicMock()
    cm.__enter__.return_value = mock_conn
    cm.__exit__.return_value = False
    monkeypatch.setattr(kl, "connect", lambda _s: cm)

    settings = MagicMock(kafka_bootstrap_servers="localhost:19092")
    stats = collect_and_store_lag(settings)

    assert stats["topic_offset_rows"] == 1
    assert stats["lag_observation_rows"] == 1

    calls = [c.args[0].strip().split()[0] for c in mock_cur.execute.call_args_list]
    assert calls.count("INSERT") == 2
