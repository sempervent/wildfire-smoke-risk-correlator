from __future__ import annotations

from collections import deque

import pytest

from wildfire_smoke import evaluate_risk


def test_evaluate_risk_strict_exits_when_no_overlap(monkeypatch) -> None:
    monkeypatch.setenv("STRICT_EVALUATION", "1")
    monkeypatch.setenv("RISK_EVAL_MODEL_VERSION", "v5")

    ops = deque(
        [
            ("fetchone", (True,)),
            ("fetchone", (5,)),
            ("fetchone", {"window_start": "2026-01-01T00:00:00+00:00", "window_end": "2026-01-02T00:00:00+00:00"}),
            ("fetchall", []),
        ]
    )

    class Cursor:
        def __init__(self) -> None:
            self.execute_calls: list[str] = []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            self.execute_calls.append(sql)

        def fetchone(self):
            kind, val = ops.popleft()
            assert kind == "fetchone"
            return val

        def fetchall(self):
            kind, val = ops.popleft()
            assert kind == "fetchall"
            return val

    class FakeConn:
        def __init__(self) -> None:
            self.committed = False

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self, *a, **k):
            return Cursor()

        def commit(self) -> None:
            self.committed = True

    monkeypatch.setattr(evaluate_risk, "connect", lambda _s: FakeConn())

    with pytest.raises(SystemExit) as ei:
        evaluate_risk.main()
    assert ei.value.code == 2
