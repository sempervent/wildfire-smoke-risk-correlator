from __future__ import annotations

from unittest.mock import MagicMock

from wildfire_smoke.compact_parse_errors import archive_candidates, summarize_candidates


def test_summarize_candidates_executes_expected_filters() -> None:
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = [("firms.hotspots.raw", "normalized.x", "JSONDecodeError", 4)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    rows = summarize_candidates(mock_conn, older_than_days=14, status="resolved")

    assert rows[0][3] == 4
    mock_cur.execute.assert_called_once()
    args = mock_cur.execute.call_args[0][1]
    assert args[0] == "resolved"
    assert args[1] == 14


def test_archive_candidates_updates_rows() -> None:
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.rowcount = 2
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    n = archive_candidates(mock_conn, older_than_days=30, status="resolved")
    assert n == 2
    mock_cur.execute.assert_called_once()
