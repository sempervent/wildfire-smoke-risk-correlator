"""
Wind corridor plume exposure approximation (``wind_v1``).

This is an engineering heuristic for dashboards and correlation experiments.
It is **not** an atmospheric dispersion model and must not be interpreted as
health guidance.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from psycopg.rows import dict_row

from wildfire_smoke.db.connection import connect
from wildfire_smoke.plume_scoring import wind_v1_exposure_components
from wildfire_smoke.settings import Settings
from wildfire_smoke.wind import angular_difference_degrees, bearing_degrees, downwind_bearing

log = logging.getLogger(__name__)

PLUME_MODEL_VERSION = "wind_v1"


def _window_bounds(settings: Settings) -> tuple[datetime, datetime]:
    window_end = datetime.now(timezone.utc).replace(microsecond=0)
    window_start = (window_end - timedelta(hours=settings.smoke_risk_lookback_hours)).replace(microsecond=0)
    return window_start, window_end


UPSERT_PLUME = """
INSERT INTO analytics.smoke_plume_exposures (
  model_version, detection_id, geography_type, geoid,
  wind_observation_id, window_start, window_end,
  distance_km, bearing_from_fire_degrees, wind_from_degrees,
  downwind_bearing_degrees, angular_error_degrees, wind_speed_mps,
  exposure_score, explanation
) VALUES (
  %s, %s, %s, %s,
  %s, %s, %s,
  %s, %s, %s,
  %s, %s, %s,
  %s, %s::jsonb
)
ON CONFLICT (detection_id, geography_type, geoid, window_start, window_end, model_version)
DO UPDATE SET
  wind_observation_id = EXCLUDED.wind_observation_id,
  distance_km = EXCLUDED.distance_km,
  bearing_from_fire_degrees = EXCLUDED.bearing_from_fire_degrees,
  wind_from_degrees = EXCLUDED.wind_from_degrees,
  downwind_bearing_degrees = EXCLUDED.downwind_bearing_degrees,
  angular_error_degrees = EXCLUDED.angular_error_degrees,
  wind_speed_mps = EXCLUDED.wind_speed_mps,
  exposure_score = EXCLUDED.exposure_score,
  explanation = EXCLUDED.explanation,
  computed_at = now();
"""


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    window_start, window_end = _window_bounds(settings)
    radius_m = settings.wind_match_radius_km * 1000.0
    lookback_half_hours = settings.wind_match_lookback_hours
    max_km = settings.plume_max_distance_km
    half_angle = settings.plume_half_angle_degrees
    max_dist_m = max_km * 1000.0

    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM analytics.smoke_plume_exposures
                WHERE window_start = %s AND window_end = %s AND model_version = %s
                """,
                (window_start, window_end, PLUME_MODEL_VERSION),
            )
        conn.commit()

    inserted = 0
    with connect(settings) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT detection_id, longitude, latitude, acq_datetime, frp
                FROM normalized.fire_detections
                WHERE acq_datetime >= %s AND acq_datetime < %s
                ORDER BY acq_datetime DESC
                LIMIT 5000
                """,
                (window_start, window_end),
            )
            fires = list(cur.fetchall())

        for fire in fires:
            det_id = str(fire["detection_id"])
            flon = float(fire["longitude"])
            flat = float(fire["latitude"])
            acq = fire["acq_datetime"]
            frp = float(fire["frp"]) if fire.get("frp") is not None else None

            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT wind_observation_id, wind_direction_degrees, wind_speed_mps, observed_at,
                           ST_DistanceSphere(
                             ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                             geom
                           ) / 1000.0 AS dist_km
                    FROM normalized.wind_observations
                    WHERE wind_direction_degrees IS NOT NULL
                      AND observed_at >= %s::timestamptz - (%s || ' hours')::interval
                      AND observed_at <= %s::timestamptz + (%s || ' hours')::interval
                      AND ST_DWithin(
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                        geom::geography,
                        %s
                      )
                    ORDER BY dist_km ASC NULLS LAST
                    LIMIT 1
                    """,
                    (
                        flon,
                        flat,
                        acq,
                        lookback_half_hours,
                        acq,
                        lookback_half_hours,
                        flon,
                        flat,
                        radius_m,
                    ),
                )
                wind_row = cur.fetchone()

            if not wind_row:
                continue

            wind_from = float(wind_row["wind_direction_degrees"])
            w_speed = float(wind_row["wind_speed_mps"]) if wind_row.get("wind_speed_mps") is not None else None
            wid = str(wind_row["wind_observation_id"])
            dw = downwind_bearing(wind_from)
            if dw is None:
                continue

            for geo_type, rel in ("county", "geo.counties"), ("tract", "geo.tracts"):
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        f"""
                        SELECT geoid,
                               ST_X(ST_Centroid(g.geom)) AS clon,
                               ST_Y(ST_Centroid(g.geom)) AS clat,
                               ST_DistanceSphere(
                                 ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                                 ST_Centroid(g.geom)
                               ) / 1000.0 AS dist_km
                        FROM {rel} g
                        WHERE ST_DWithin(
                          ST_Centroid(g.geom)::geography,
                          ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                          %s
                        )
                        """,
                        (flon, flat, flon, flat, max_dist_m),
                    )
                    candidates = cur.fetchall()

                for row in candidates:
                    geoid = str(row["geoid"])
                    clon = float(row["clon"])
                    clat = float(row["clat"])
                    dist_km = float(row["dist_km"])
                    bear = bearing_degrees(flon, flat, clon, clat)
                    angular_err = angular_difference_degrees(bear, dw)
                    if angular_err > half_angle:
                        continue

                    score, comp_expl = wind_v1_exposure_components(
                        distance_km=dist_km,
                        plume_max_distance_km=max_km,
                        angular_error_degrees=angular_err,
                        plume_half_angle_degrees=half_angle,
                        wind_speed_mps=w_speed,
                        frp=frp,
                    )

                    explanation: dict[str, Any] = {
                        "detection_id": det_id,
                        "wind_observation_id": wid,
                        "components": comp_expl,
                        "half_angle_degrees": half_angle,
                        "plume_max_distance_km": max_km,
                        "disclaimer": "engineering corridor approximation; not dispersion modeling",
                    }

                    with conn.cursor() as cur:
                        cur.execute(
                            UPSERT_PLUME,
                            (
                                PLUME_MODEL_VERSION,
                                det_id,
                                geo_type,
                                geoid,
                                wid,
                                window_start,
                                window_end,
                                dist_km,
                                bear,
                                wind_from,
                                dw,
                                angular_err,
                                w_speed,
                                score,
                                json.dumps(explanation),
                            ),
                        )
                    inserted += 1

        conn.commit()

    log.info(
        "plume_exposure_complete",
        extra={
            "fires_considered": len(fires),
            "rows_upserted": inserted,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "model_version": PLUME_MODEL_VERSION,
        },
    )


if __name__ == "__main__":
    main()
