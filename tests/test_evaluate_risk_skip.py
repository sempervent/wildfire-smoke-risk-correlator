from __future__ import annotations

from wildfire_smoke import evaluate_risk


def test_evaluate_risk_skips_when_table_missing(monkeypatch, capsys) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self._step = 0

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, *a, **k) -> None:
            return None

        def fetchone(self):
            self._step += 1
            if self._step == 1:
                return (False,)
            raise AssertionError("should not reach second query")

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self, *a, **k):
            return FakeCursor()

        def commit(self) -> None:
            raise AssertionError("no commit")

    monkeypatch.setattr(evaluate_risk, "connect", lambda _s: FakeConn())
    evaluate_risk.main()
    out = capsys.readouterr().out
    assert "not installed" in out
