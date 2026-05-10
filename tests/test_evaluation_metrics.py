from __future__ import annotations

import math

from wildfire_smoke.evaluation_metrics import (
    binary_prf_counts,
    evaluation_confidence_label,
    pearson_correlation,
    precision_recall_like,
)


def test_pearson_requires_three_pairs() -> None:
    assert pearson_correlation([1.0, 2.0], [1.0, 4.0]) is None
    assert pearson_correlation([1.0, 2.0, 3.0], [1.0, 4.0, 3.0]) is not None


def test_pearson_zero_variance() -> None:
    assert pearson_correlation([2.0, 2.0, 2.0], [1.0, 2.0, 3.0]) is None


def test_pearson_perfect_positive() -> None:
    r = pearson_correlation([1.0, 2.0, 3.0], [2.0, 4.0, 6.0])
    assert r is not None and math.isclose(r, 1.0, abs_tol=1e-9)


def test_evaluation_confidence_labels() -> None:
    assert evaluation_confidence_label(match_count=0, min_match=3, correlation_computed=False) == "no_observations"
    assert evaluation_confidence_label(match_count=2, min_match=3, correlation_computed=False) == "insufficient_samples"
    assert evaluation_confidence_label(match_count=5, min_match=3, correlation_computed=False) == "exploratory"
    assert evaluation_confidence_label(match_count=15, min_match=3, correlation_computed=True) == "moderate_signal"
    assert evaluation_confidence_label(match_count=40, min_match=3, correlation_computed=True) == "strong_signal"


def test_binary_prf_and_precision_recall() -> None:
    preds = [80.0, 40.0, 80.0]
    obs = [50.0, 50.0, 20.0]
    tp, fp, fn = binary_prf_counts(
        preds, obs, pred_high_threshold=70.0, obs_high_threshold=35.0
    )
    assert (tp, fp, fn) == (1, 1, 1)
    prec, rec = precision_recall_like(tp, fp, fn)
    assert prec is not None and rec is not None
    assert math.isclose(prec, 0.5) and math.isclose(rec, 0.5)
