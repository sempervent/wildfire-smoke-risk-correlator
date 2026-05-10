"""Compare latest v4 risk scores to analytics.risk_observations (Phase 10 hook)."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from wildfire_smoke.db.connection import connect
from wildfire_smoke.logging import configure_logging
from wildfire_smoke.settings import Settings

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    obs_type = os.environ.get("RISK_EVAL_OBSERVATION_TYPE", "risk_score").strip()

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
            return

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT window_start, window_end
                FROM analytics.smoke_risk_scores
                WHERE model_version = 'v4'
                ORDER BY computed_at DESC NULLS LAST
                LIMIT 1
                """
            )
            win = cur.fetchone()
        if not win:
            print("No v4 smoke_risk_scores rows; skipping comparative metrics (exit 0).")
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
                WHERE s.model_version = 'v4'
                  AND s.window_start = %s AND s.window_end = %s
                  AND o.observed_at <= %s
                """,
                (obs_type, ws, we, we),
            )
            pairs = list(cur.fetchall())

        if not pairs:
            print("No overlapping observations for latest v4 window (exit 0).")
            return

        errs: list[float] = []
        sq: list[float] = []
        for p in pairs:
            if p.get("observed") is None:
                continue
            pred = float(p["predicted"])
            obs = float(p["observed"])
            errs.append(abs(pred - obs))
            sq.append((pred - obs) ** 2)

        mae = sum(errs) / len(errs) if errs else None
        rmse = (sum(sq) / len(sq)) ** 0.5 if sq else None

        summary: dict[str, Any] = {
            "observation_type": obs_type,
            "pairs": len(pairs),
            "mae": mae,
            "rmse": rmse,
        }

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics.risk_model_evaluations (
                  model_version, window_start, window_end, observation_type,
                  match_count, mae, rmse, correlation, summary
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                ("v4", ws, we, obs_type, len(pairs), mae, rmse, None, Json(summary)),
            )
        conn.commit()

    log.info("risk_evaluation_complete", extra=summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
