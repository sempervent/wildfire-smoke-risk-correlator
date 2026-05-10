"""
AQ lag-window summaries vs dispersion exposure — Phase 11/12 evaluation scaffolding only.

Persists explicit evidence labels (no_data vs weak vs alignment heuristics); not validation.
"""

from __future__ import annotations

import json
import logging
import os
from collections import Counter
from datetime import timedelta

from psycopg.rows import dict_row

from wildfire_smoke.calibration_evidence import classify_dispersion_aq_evidence, parse_lag_windows_hours
from wildfire_smoke.db.connection import connect
from wildfire_smoke.settings import Settings

log = logging.getLogger(__name__)

UPSERT_COMPARE = """
INSERT INTO analytics.dispersion_aq_comparisons (
  model_version, geography_type, geoid, window_start, window_end,
  lag_bucket, lag_hours_lo, lag_hours_hi,
  max_dispersion_score, avg_dispersion_score,
  max_risk_score_v5, avg_risk_score_v5,
  avg_pm25, avg_pm10, aq_observation_count,
  fire_detection_count, dispersion_exposure_count,
  lag_window, lag_hours, comparison_score, evidence_label, explanation
) VALUES (
  %s, %s, %s, %s, %s,
  %s, %s, %s,
  %s, %s,
  %s, %s,
  %s, %s, %s,
  %s, %s,
  %s, %s, %s, %s, %s::jsonb
)
ON CONFLICT (model_version, geography_type, geoid, window_start, window_end, lag_bucket)
DO UPDATE SET
  max_dispersion_score = EXCLUDED.max_dispersion_score,
  avg_dispersion_score = EXCLUDED.avg_dispersion_score,
  max_risk_score_v5 = EXCLUDED.max_risk_score_v5,
  avg_risk_score_v5 = EXCLUDED.avg_risk_score_v5,
  avg_pm25 = EXCLUDED.avg_pm25,
  avg_pm10 = EXCLUDED.avg_pm10,
  aq_observation_count = EXCLUDED.aq_observation_count,
  fire_detection_count = EXCLUDED.fire_detection_count,
  dispersion_exposure_count = EXCLUDED.dispersion_exposure_count,
  lag_window = EXCLUDED.lag_window,
  lag_hours = EXCLUDED.lag_hours,
  comparison_score = EXCLUDED.comparison_score,
  evidence_label = EXCLUDED.evidence_label,
  explanation = EXCLUDED.explanation,
  computed_at = now();
"""


def _lag_buckets(settings: Settings) -> tuple[tuple[str, float, float], ...]:
    raw = os.environ.get("CALIBRATION_LAG_WINDOWS_HOURS", settings.calibration_lag_windows_hours)
    parsed = parse_lag_windows_hours(raw)
    if parsed:
        return parsed
    return (
        ("0-3h", 0.0, 3.0),
        ("3-6h", 3.0, 6.0),
        ("6-12h", 6.0, 12.0),
        ("12-24h", 12.0, 24.0),
    )


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()

    if not settings.dispersion_enabled:
        print("DISPERSION_ENABLED=0; skipping dispersion AQ comparison (exit 0).")
        return

    model_version = settings.dispersion_model_version
    min_aq = settings.calibration_min_aq_observations
    hi_pm = settings.calibration_high_pm25_threshold
    lo_pm = settings.calibration_low_pm25_threshold
    hi_disp = settings.calibration_high_dispersion_score
    lo_disp = settings.calibration_low_dispersion_score
    lag_buckets = _lag_buckets(settings)

    with connect(settings) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT window_start, window_end
                FROM analytics.smoke_dispersion_exposures
                WHERE model_version = %s
                ORDER BY window_end DESC NULLS LAST
                LIMIT 1
                """,
                (model_version,),
            )
            win = cur.fetchone()

        if not win:
            print("No dispersion exposures; nothing to compare (exit 0).")
            return

        window_start, window_end = win["window_start"], win["window_end"]

        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM analytics.dispersion_aq_comparisons
                WHERE model_version = %s AND window_start = %s AND window_end = %s
                """,
                (model_version, window_start, window_end),
            )
        conn.commit()

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT geography_type, geoid,
                       MAX(dispersion_score)::double precision AS max_dispersion,
                       AVG(dispersion_score)::double precision AS avg_dispersion,
                       COUNT(*)::int AS dispersion_exposure_count,
                       COUNT(DISTINCT detection_id)::int AS fire_detection_count
                FROM analytics.smoke_dispersion_exposures
                WHERE model_version = %s
                  AND window_start = %s AND window_end = %s
                GROUP BY geography_type, geoid
                """,
                (model_version, window_start, window_end),
            )
            geos = list(cur.fetchall())

        risk_by_geo: dict[tuple[str, str], dict[str, float | None]] = {}
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT geography_type, geoid,
                       MAX(risk_score)::double precision AS max_risk,
                       AVG(risk_score)::double precision AS avg_risk
                FROM analytics.smoke_risk_scores
                WHERE model_version = 'v5'
                  AND window_start = %s AND window_end = %s
                GROUP BY geography_type, geoid
                """,
                (window_start, window_end),
            )
            for r in cur.fetchall():
                risk_by_geo[(str(r["geography_type"]), str(r["geoid"]))] = {
                    "max_risk": float(r["max_risk"]) if r["max_risk"] is not None else None,
                    "avg_risk": float(r["avg_risk"]) if r["avg_risk"] is not None else None,
                }

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::bigint FROM normalized.air_quality_measurements")
            row = cur.fetchone()
            aq_total = int(row[0]) if row and row[0] is not None else 0

        written = 0
        evidence_counts: Counter[str] = Counter()

        for g in geos:
            geo_type = str(g["geography_type"])
            geoid = str(g["geoid"])
            max_disp = float(g["max_dispersion"] or 0.0)
            avg_disp = float(g["avg_dispersion"] or 0.0)
            disp_n = int(g["dispersion_exposure_count"] or 0)
            fire_n = int(g["fire_detection_count"] or 0)
            rk = risk_by_geo.get((geo_type, geoid), {})
            max_r5 = rk.get("max_risk")
            avg_r5 = rk.get("avg_risk")
            geo_col = "county_geoid" if geo_type == "county" else "tract_geoid"

            for label, lo_h, hi_h in lag_buckets:
                t0 = window_end + timedelta(hours=lo_h)
                t1 = window_end + timedelta(hours=hi_h)
                mid_lag = (lo_h + hi_h) / 2.0

                if aq_total == 0:
                    n = 0
                    avg25 = None
                    avg10 = None
                else:
                    with conn.cursor(row_factory=dict_row) as cur:
                        cur.execute(
                            f"""
                            SELECT
                              AVG(value) FILTER (WHERE parameter = 'pm25')::double precision AS avg_pm25,
                              AVG(value) FILTER (WHERE parameter = 'pm10')::double precision AS avg_pm10,
                              COUNT(*)::int AS n
                            FROM normalized.air_quality_measurements
                            WHERE {geo_col} = %s
                              AND measured_at >= %s AND measured_at < %s
                            """,
                            (geoid, t0, t1),
                        )
                        aq = cur.fetchone()

                    n = int(aq["n"] or 0) if aq else 0
                    avg25 = float(aq["avg_pm25"]) if aq and aq.get("avg_pm25") is not None else None
                    avg10 = float(aq["avg_pm10"]) if aq and aq.get("avg_pm10") is not None else None

                evidence = classify_dispersion_aq_evidence(
                    aq_observation_count=n,
                    min_aq_observations=min_aq,
                    dispersion_exposure_count=disp_n,
                    max_dispersion_score=max_disp,
                    avg_pm25=avg25,
                    high_pm25=hi_pm,
                    low_pm25=lo_pm,
                    high_dispersion_score=hi_disp,
                    low_dispersion_score=lo_disp,
                )
                evidence_counts[evidence] += 1

                comparison_score = None
                if n >= min_aq and evidence not in {"no_aq_data", "insufficient_aq_data"}:
                    disp_norm = min(1.0, max_disp / 100.0)
                    pm_norm = min(1.0, max((avg25 or 0.0) / 50.0, (avg10 or 0.0) / 100.0))
                    comparison_score = abs(disp_norm - pm_norm) * 100.0

                expl = {
                    "lag_bucket": label,
                    "note": "engineering divergence / evidence label — not validated correlation",
                    "disp_norm": min(1.0, max_disp / 100.0) if n >= min_aq else None,
                    "pm_norm": (
                        min(1.0, max((avg25 or 0.0) / 50.0, (avg10 or 0.0) / 100.0)) if n else None
                    ),
                    "evidence_label": evidence,
                    "calibration_thresholds": {
                        "min_aq_observations": min_aq,
                        "high_pm25": hi_pm,
                        "low_pm25": lo_pm,
                        "high_dispersion_score": hi_disp,
                        "low_dispersion_score": lo_disp,
                    },
                }

                with conn.cursor() as cur:
                    cur.execute(
                        UPSERT_COMPARE,
                        (
                            model_version,
                            geo_type,
                            geoid,
                            window_start,
                            window_end,
                            label,
                            lo_h,
                            hi_h,
                            max_disp,
                            avg_disp,
                            max_r5,
                            avg_r5,
                            avg25,
                            avg10,
                            n,
                            fire_n,
                            disp_n,
                            label,
                            mid_lag,
                            comparison_score,
                            evidence,
                            json.dumps(expl),
                        ),
                    )
                written += 1

        conn.commit()

    out = {
        "dispersion_aq_comparison_rows_upserted": written,
        "evidence_label_counts": dict(evidence_counts),
        "lag_buckets": [x[0] for x in lag_buckets],
    }
    print(json.dumps(out, indent=2))
    log.info("compare_dispersion_aq_complete", extra=out)


if __name__ == "__main__":
    main()
