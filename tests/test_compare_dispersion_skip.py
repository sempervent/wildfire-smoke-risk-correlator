from __future__ import annotations

from wildfire_smoke.spark.compare_dispersion_aq import main


def test_compare_dispersion_aq_disabled_exits_early(monkeypatch, capsys) -> None:
    monkeypatch.setenv("DISPERSION_ENABLED", "0")
    main()
    out = capsys.readouterr().out.lower()
    assert "skip" in out
