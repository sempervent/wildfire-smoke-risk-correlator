from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from wildfire_smoke.ingestion_runs import create_run, finish_run


def _mock_conn_with_cursor(mock_cur: MagicMock) -> MagicMock:
    mock_conn = MagicMock()
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_cur
    mock_cm.__exit__.return_value = None
    mock_conn.cursor.return_value = mock_cm
    return mock_conn


def test_create_run_inserts_and_commits() -> None:
    run_id = uuid.uuid4()
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = (run_id,)
    mock_conn = _mock_conn_with_cursor(mock_cur)

    out = create_run(mock_conn, source="firms", mode="live", config={"bbox": "a"})

    assert out == run_id
    mock_cur.execute.assert_called_once()
    mock_conn.commit.assert_called_once()


def test_create_run_rejects_bad_mode() -> None:
    mock_conn = _mock_conn_with_cursor(MagicMock())
    with pytest.raises(ValueError, match="invalid ingestion mode"):
        create_run(mock_conn, source="firms", mode="staging", config={})


def test_finish_run_updates_and_commits() -> None:
    mock_cur = MagicMock()
    mock_conn = _mock_conn_with_cursor(mock_cur)
    run_id = uuid.uuid4()

    finish_run(
        mock_conn,
        run_id,
        status="succeeded",
        records_fetched=10,
        records_published=10,
        records_failed=0,
    )

    mock_cur.execute.assert_called_once()
    mock_conn.commit.assert_called_once()


def test_finish_run_rejects_running_status() -> None:
    mock_conn = _mock_conn_with_cursor(MagicMock())
    with pytest.raises(ValueError, match="succeeded or failed"):
        finish_run(
            mock_conn,
            uuid.uuid4(),
            status="running",
            records_fetched=0,
            records_published=0,
            records_failed=0,
        )
