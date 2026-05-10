"""Compare risk scores to analytics.risk_observations (Phase 10/12 hook — not validation)."""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from wildfire_smoke.db.connection import connect
from wildfire_smoke.evaluation_metrics import (
    binary_prf_counts,
    evaluation_confidence_label,
    pearson_correlation,
    precision_recall_like,
)
from wildfire_smoke.logging import configure_logging
from wildfire_smoke.settings import Settings

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    obs_type = os.environ.get("RISK_EVAL_OBSERVATION_TYPE", "risk_score").strip()
    eval_model = os.environ.get("RISK_EVAL_MODEL_VERSION", settings.smoke_risk_model_version).strip().lower()
    if eval_model not in {"v1", "v2", "v3", "v4", "v5"}:
        print(f"Invalid RISK_EVAL_MODEL_VERSION={eval_model!r}; skipping.", file=sys.stderr)
        return

    min_match = settings.risk_eval_min_match_count
    thr_risk = settings.risk_eval_high_risk_threshold
    thr_obs = settings.risk_eval_high_obs_threshold

    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema = 'analytics' AND table_name = 'risk_observations'
                )
                """
            )
            if not cur.fetchone()[0]:
                print("analytics.risk_observations not installed; skip evaluation (exit 0).")
                return
            cur.execute("SELECT COUNT(*) FROM analytics.risk_observations")
            n_obs = int(cur.fetchone()[0])

        if n_obs == 0:
            print("No rows in analytics.risk_observations; nothing to evaluate (exit 0).")
            if os.environ.get("STRICT_EVALUATION", "0").strip().lower() in {"1", "true", "yes"}:
                print("STRICT_EVALUATION=1: no risk_observations rows", file=sys.stderr)
                raise SystemExit(2)
            return

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT window_start, window_end
                FROM analytics.smoke_risk_scores
                WHERE model_version = %s
                ORDER BY computed_at DESC NULLS LAST
                LIMIT 1
                """,
                (eval_model,),
            )
            win = cur.fetchone()

        if not win:
            print(f"No smoke_risk_scores rows for model_version={eval_model}; skipping (exit 0).")
            if os.environ.get("STRICT_EVALUATION", "0").strip().lower() in {"1", "true", "yes"}:
                print(f"STRICT_EVALUATION=1: no risk scores for {eval_model}", file=sys.stderr)
                raise SystemExit(2)
            return

        ws, we = win["window_start"], win["window_end"]

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT s.geography_type, s.geoid, s.risk_score AS predicted,
                       o.observed_value AS observed
                FROM analytics.smoke_risk_scores s
                JOIN analytics.risk_observations o
                  ON o.geography_type = s.geography_type
                 AND o.geoid = s.geoid
                 AND o.observation_type = %s
                WHERE s.model_version = %s
                  AND s.window_start = %s AND s.window_end = %s
                  AND o.observed_at <= %s
                """,
                (obs_type, eval_model, ws, we, we),
            )
            pairs = list(cur.fetchall())

        if not pairs:
            if os.environ.get("STRICT_EVALUATION", "0").strip().lower() in {"1", "true", "yes"}:
                print("STRICT_EVALUATION=1: no_overlapping_pairs_for_window", file=sys.stderr)
                raise SystemExit(2)

        valid = [(float(p["predicted"]), float(p["observed"])) for p in pairs if p.get("observed") is not None]
        match_count = len(valid)

        insufficient_reason: str | None = None
        if not pairs:
            insufficient_reason = "no_overlapping_pairs_for_window"
        elif match_count < min_match:
            insufficient_reason = "below_risk_eval_min_match_count"

        preds = [a for a, _ in valid]
        obsv = [b for _, b in valid]

        mae = sum(abs(a - b) for a, b in valid) / match_count if valid else None
        rmse = (sum((a - b) ** 2 for a, b in valid) / match_count) ** 0.5 if valid else None

        corr: float | None = None
        if match_count >= min_match:
            corr = pearson_correlation(preds, obsv)

        tp = fp = fn = 0
        prec = rec = None
        if valid:
            tp, fp, fn = binary_prf_counts(preds, obsv, pred_high_threshold=thr_risk, obs_high_threshold=thr_obs)
            prec, rec = precision_recall_like(tp, fp, fn)

        correlation_computed = corr is not None
        confidence = evaluation_confidence_label(
            match_count=match_count,
            min_match=min_match,
            correlation_computed=correlation_computed,
        )

        summary: dict[str, Any] = {
            "observation_type": obs_type,
            "risk_eval_model_version": eval_model,
            "pairs_joined": len(pairs),
            "pairs_with_observed_value": match_count,
            "mae": mae,
            "rmse": rmse,
            "correlation": corr,
            "precision_like_high_risk": prec,
            "recall_like_high_risk": rec,
            "false_positive_like_count": fp,
            "false_negative_like_count": fn,
            "risk_eval_min_match_count": min_match,
            "risk_eval_high_risk_threshold": thr_risk,
            "risk_eval_high_obs_threshold": thr_obs,
        }

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics.risk_model_evaluations (
                  model_version, window_start, window_end, observation_type,
                  match_count, mae, rmse, correlation,
                  precision_like_high_risk, recall_like_high_risk,
                  false_positive_like_count, false_negative_like_count,
                  insufficient_data_reason, confidence_label,
                  summary
                ) VALUES (
                  %s, %s, %s, %s,
                  %s, %s, %s, %s,
                  %s, %s,
                  %s, %s,
                  %s, %s,
                  %s
                )
                """,
                (
                    eval_model,
                    ws,
                    we,
                    obs_type,
                    match_count,
                    mae,
                    rmse,
                    corr,
                    prec,
                    rec,
                    fp,
                    fn,
                    insufficient_reason,
                    confidence,
                    Json(summary),
                ),
            )
        conn.commit()

    log.info("risk_evaluation_complete", extra=summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
