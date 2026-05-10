"""
AQ lag-window summaries vs dispersion exposure — Phase 11 evaluation scaffolding only.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta

from psycopg.rows import dict_row

from wildfire_smoke.db.connection import connect
from wildfire_smoke.settings import Settings

log = logging.getLogger(__name__)

UPSERT_COMPARE = """
INSERT INTO analytics.dispersion_aq_comparisons (
  model_version, geography_type, geoid, window_start, window_end,
  lag_bucket, lag_hours_lo, lag_hours_hi,
  max_dispersion_score, avg_pm25, avg_pm10, aq_observation_count,
  lag_hours, comparison_score, explanation
) VALUES (
  %s, %s, %s, %s, %s,
  %s, %s, %s,
  %s, %s, %s, %s,
  %s, %s, %s::jsonb
)
ON CONFLICT (model_version, geography_type, geoid, window_start, window_end, lag_bucket)
DO UPDATE SET
  max_dispersion_score = EXCLUDED.max_dispersion_score,
  avg_pm25 = EXCLUDED.avg_pm25,
  avg_pm10 = EXCLUDED.avg_pm10,
  aq_observation_count = EXCLUDED.aq_observation_count,
  lag_hours = EXCLUDED.lag_hours,
  comparison_score = EXCLUDED.comparison_score,
  explanation = EXCLUDED.explanation,
  computed_at = now();
"""


LAG_BUCKETS: tuple[tuple[str, float, float], ...] = (
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
                       MAX(dispersion_score)::double precision AS max_dispersion
                FROM analytics.smoke_dispersion_exposures
                WHERE model_version = %s
                  AND window_start = %s AND window_end = %s
                GROUP BY geography_type, geoid
                """,
                (model_version, window_start, window_end),
            )
            geos = list(cur.fetchall())

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::bigint FROM normalized.air_quality_measurements")
            row = cur.fetchone()
            aq_total = int(row[0]) if row and row[0] is not None else 0

        if aq_total == 0:
            print("No AQ measurements in database; skip dispersion AQ comparison (exit 0).")
            return

        written = 0
        for g in geos:
            geo_type = str(g["geography_type"])
            geoid = str(g["geoid"])
            max_disp = float(g["max_dispersion"] or 0.0)
            geo_col = "county_geoid" if geo_type == "county" else "tract_geoid"

            for label, lo_h, hi_h in LAG_BUCKETS:
                t0 = window_end + timedelta(hours=lo_h)
                t1 = window_end + timedelta(hours=hi_h)
                mid_lag = (lo_h + hi_h) / 2.0

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

                if n < 2:
                    continue

                disp_norm = min(1.0, max_disp / 100.0)
                pm_norm = min(1.0, max((avg25 or 0.0) / 50.0, (avg10 or 0.0) / 100.0))
                comparison_score = abs(disp_norm - pm_norm) * 100.0

                expl = {
                    "lag_bucket": label,
                    "note": "engineering divergence metric — not validated correlation",
                    "disp_norm": disp_norm,
                    "pm_norm": pm_norm,
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
                            avg25,
                            avg10,
                            n,
                            mid_lag,
                            comparison_score,
                            json.dumps(expl),
                        ),
                    )
                written += 1

        conn.commit()

    print(json.dumps({"dispersion_aq_comparison_rows_upserted": written}, indent=2))
    log.info("compare_dispersion_aq_complete", extra={"rows": written})


if __name__ == "__main__":
    main()
