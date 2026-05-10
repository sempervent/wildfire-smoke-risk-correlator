"""Match recent fire detections to nearest gridded weather cells (Phase 9)."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from psycopg.rows import dict_row

from wildfire_smoke.db.connection import connect
from wildfire_smoke.settings import Settings

log = logging.getLogger(__name__)


def _window_bounds(settings: Settings) -> tuple[datetime, datetime]:
    window_end = datetime.now(timezone.utc).replace(microsecond=0)
    window_start = (window_end - timedelta(hours=settings.smoke_risk_lookback_hours)).replace(microsecond=0)
    return window_start, window_end


UPSERT_MATCH = """
INSERT INTO analytics.fire_weather_matches (
  detection_id, weather_cell_id, match_method,
  distance_km, time_delta_minutes,
  wind_speed_mps, wind_direction_degrees, temperature_c, relative_humidity_percent
) VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (detection_id, weather_cell_id, match_method) DO UPDATE SET
  distance_km = EXCLUDED.distance_km,
  time_delta_minutes = EXCLUDED.time_delta_minutes,
  wind_speed_mps = EXCLUDED.wind_speed_mps,
  wind_direction_degrees = EXCLUDED.wind_direction_degrees,
  temperature_c = EXCLUDED.temperature_c,
  relative_humidity_percent = EXCLUDED.relative_humidity_percent,
  matched_at = now();
"""


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    window_start, window_end = _window_bounds(settings)
    radius_m = settings.fire_weather_match_radius_km * 1000.0
    max_dt_sec = settings.fire_weather_match_max_time_delta_hours * 3600.0
    method = settings.fire_weather_match_method

    detections_considered = 0
    matches_written = 0
    unmatched = 0

    with connect(settings) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                DELETE FROM analytics.fire_weather_matches
                WHERE detection_id IN (
                  SELECT detection_id FROM normalized.fire_detections
                  WHERE acq_datetime >= %s AND acq_datetime < %s
                )
                """,
                (window_start, window_end),
            )
        conn.commit()

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT detection_id, longitude, latitude, acq_datetime,
                       ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom
                FROM normalized.fire_detections
                WHERE acq_datetime >= %s AND acq_datetime < %s
                ORDER BY acq_datetime DESC
                LIMIT 5000
                """,
                (window_start, window_end),
            )
            fires = list(cur.fetchall())

        detections_considered = len(fires)

        for fire in fires:
            det_id = str(fire["detection_id"])
            acq = fire["acq_datetime"]
            flon = float(fire["longitude"])
            flat = float(fire["latitude"])

            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT c.weather_cell_id,
                           ST_DistanceSphere(ST_SetSRID(ST_MakePoint(%s, %s), 4326), c.geom) / 1000.0 AS distance_km,
                           EXTRACT(EPOCH FROM (c.valid_time - %s::timestamptz)) / 60.0 AS time_delta_minutes,
                           c.wind_speed_mps,
                           c.wind_direction_degrees,
                           c.temperature_c,
                           c.relative_humidity_percent
                    FROM normalized.weather_grid_cells c
                    WHERE c.wind_direction_degrees IS NOT NULL
                      AND abs(EXTRACT(EPOCH FROM (c.valid_time - %s::timestamptz))) <= %s
                      AND ST_DWithin(
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                        c.geom::geography,
                        %s
                      )
                    ORDER BY
                      ST_DistanceSphere(ST_SetSRID(ST_MakePoint(%s, %s), 4326), c.geom) ASC,
                      abs(EXTRACT(EPOCH FROM (c.valid_time - %s::timestamptz))) ASC
                    LIMIT 1
                    """,
                    (
                        flon,
                        flat,
                        acq,
                        acq,
                        max_dt_sec,
                        flon,
                        flat,
                        radius_m,
                        flon,
                        flat,
                        acq,
                    ),
                )
                hit = cur.fetchone()

            if not hit:
                unmatched += 1
                continue

            with conn.cursor() as cur:
                cur.execute(
                    UPSERT_MATCH,
                    (
                        det_id,
                        str(hit["weather_cell_id"]),
                        method,
                        float(hit["distance_km"]) if hit.get("distance_km") is not None else None,
                        float(hit["time_delta_minutes"]) if hit.get("time_delta_minutes") is not None else None,
                        float(hit["wind_speed_mps"]) if hit.get("wind_speed_mps") is not None else None,
                        float(hit["wind_direction_degrees"]) if hit.get("wind_direction_degrees") is not None else None,
                        float(hit["temperature_c"]) if hit.get("temperature_c") is not None else None,
                        float(hit["relative_humidity_percent"]) if hit.get("relative_humidity_percent") is not None else None,
                    ),
                )
            matches_written += 1

        conn.commit()

    log.info(
        "match_fire_weather_complete",
        extra={
            "detections_considered": detections_considered,
            "matches_written": matches_written,
            "unmatched_detections": unmatched,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        },
    )


if __name__ == "__main__":
    main()
