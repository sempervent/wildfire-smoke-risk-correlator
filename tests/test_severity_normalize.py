from __future__ import annotations

import pytest

from wildfire_smoke.severity import normalize_db_severity, passes_min_severity, severity_rank


def test_normalize_warn_vs_high_smoke() -> None:
    assert normalize_db_severity("high_smoke_risk", "warn") == "high"
    assert normalize_db_severity("high_smoke_risk", "critical") == "critical"


def test_normalize_warn_vs_high_plume() -> None:
    assert normalize_db_severity("high_plume_exposure", "warn") == "high"


def test_normalize_generic_warn_is_warning() -> None:
    assert normalize_db_severity("stale_firms_normalized", "warn") == "warning"


def test_severity_rank_ordering() -> None:
    assert severity_rank("info") < severity_rank("warning")
    assert severity_rank("warning") < severity_rank("high")
    assert severity_rank("high") < severity_rank("critical")


def test_passes_min_severity_filter() -> None:
    assert passes_min_severity("critical", "high") is True
    assert passes_min_severity("high", "high") is True
    assert passes_min_severity("warning", "high") is False


def test_unknown_severity_errors() -> None:
    with pytest.raises(ValueError):
        severity_rank("nope")
