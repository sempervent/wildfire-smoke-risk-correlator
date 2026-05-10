"""Risk vs observation evaluation helpers (engineering metrics only)."""

from __future__ import annotations


def pearson_correlation(xs: list[float], ys: list[float]) -> float | None:
    """Pearson r; requires ≥3 pairs and variance in both dimensions."""
    n = len(xs)
    if n != len(ys) or n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0.0 or vy <= 0.0:
        return None
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    return cov / (vx**0.5 * vy**0.5)


def evaluation_confidence_label(*, match_count: int, min_match: int, correlation_computed: bool) -> str:
    """Qualitative batch confidence — not statistical power analysis."""
    if match_count == 0:
        return "no_observations"
    if match_count < min_match:
        return "insufficient_samples"
    if match_count < 10 or not correlation_computed:
        return "exploratory"
    if match_count < 30:
        return "moderate_signal"
    return "strong_signal"


def binary_prf_counts(
    predictions: list[float],
    observations: list[float],
    *,
    pred_high_threshold: float,
    obs_high_threshold: float,
) -> tuple[int, int, int]:
    """Return TP, FP, FN for high/high heuristic labels."""
    tp = fp = fn = 0
    for pr, ob in zip(predictions, observations, strict=True):
        ph = pr >= pred_high_threshold
        oh = ob >= obs_high_threshold
        if ph and oh:
            tp += 1
        elif ph and not oh:
            fp += 1
        elif oh and not ph:
            fn += 1
    return tp, fp, fn


def precision_recall_like(tp: int, fp: int, fn: int) -> tuple[float | None, float | None]:
    prec = tp / (tp + fp) if (tp + fp) > 0 else None
    rec = tp / (tp + fn) if (tp + fn) > 0 else None
    return prec, rec
