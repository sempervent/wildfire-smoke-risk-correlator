"""
Bounded Gaussian dispersion proxy (``gaussian_v0``) — Phase 11.

Not HYSPLIT / not regulatory / not health guidance. See ``wildfire_smoke.dispersion``.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from psycopg.rows import dict_row

from wildfire_smoke.db.connection import connect
from wildfire_smoke.dispersion import (
    dispersion_concentration_proxy,
    dispersion_score_from_proxy,
    gaussian_weight,
    source_strength_from_fire,
    wind_aligned_components,
)
from wildfire_smoke.settings import Settings
from wildfire_smoke.spark.compute_plume_exposure import _fetch_grid_wind, _fetch_station_wind
from wildfire_smoke.wind import bearing_degrees, downwind_bearing

log = logging.getLogger(__name__)


def _window_bounds(settings: Settings) -> tuple[datetime, datetime]:
    window_end = datetime.now(timezone.utc).replace(microsecond=0)
    window_start = (window_end - timedelta(hours=settings.dispersion_lookback_hours)).replace(microsecond=0)
    return window_start, window_end


UPSERT_DISPERSION = """
INSERT INTO analytics.smoke_dispersion_exposures (
  model_version, detection_id, geography_type, geoid,
  weather_cell_id, wind_observation_id,
  window_start, window_end,
  distance_km, downwind_distance_km, crosswind_distance_km,
  bearing_from_fire_degrees, wind_from_degrees, downwind_bearing_degrees,
  wind_speed_mps, source_strength, dispersion_score, concentration_proxy, explanation
) VALUES (
  %s, %s, %s, %s,
  %s, %s,
  %s, %s,
  %s, %s, %s,
  %s, %s, %s,
  %s, %s, %s, %s, %s::jsonb
)
ON CONFLICT (model_version, detection_id, geography_type, geoid, window_start, window_end)
DO UPDATE SET
  weather_cell_id = EXCLUDED.weather_cell_id,
  wind_observation_id = EXCLUDED.wind_observation_id,
  distance_km = EXCLUDED.distance_km,
  downwind_distance_km = EXCLUDED.downwind_distance_km,
  crosswind_distance_km = EXCLUDED.crosswind_distance_km,
  bearing_from_fire_degrees = EXCLUDED.bearing_from_fire_degrees,
  wind_from_degrees = EXCLUDED.wind_from_degrees,
  downwind_bearing_degrees = EXCLUDED.downwind_bearing_degrees,
  wind_speed_mps = EXCLUDED.wind_speed_mps,
  source_strength = EXCLUDED.source_strength,
  dispersion_score = EXCLUDED.dispersion_score,
  concentration_proxy = EXCLUDED.concentration_proxy,
  explanation = EXCLUDED.explanation,
  computed_at = now();
"""


def _resolve_wind_dispersion(
    conn,
    settings: Settings,
    fire: dict[str, Any],
) -> tuple[dict[str, Any] | None, bool]:
    if settings.dispersion_use_grid_weather:
        g = _fetch_grid_wind(conn, settings, fire)
        if g is not None:
            return g, False
        if settings.dispersion_fallback_to_station_wind:
            w = _fetch_station_wind(conn, settings, fire)
            return w, True
        return None, False
    w = _fetch_station_wind(conn, settings, fire)
    return w, False


def _assert_tract_guard(conn, settings: Settings) -> None:
    if settings.smoke_risk_geographies not in {"tract", "both"}:
        return
    if settings.dispersion_allow_large_run:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*)::bigint FROM geo.tracts")
        n = int(cur.fetchone()[0])
    if n > 8000:
        raise ValueError(
            "geo.tracts count exceeds safe default for dispersion (8000). "
            "Set DISPERSION_ALLOW_LARGE_RUN=1 to acknowledge a large tract corpus."
        )


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()

    if not settings.dispersion_enabled:
        print("DISPERSION_ENABLED=0; skipping dispersion computation (exit 0).")
        return

    model_version = settings.dispersion_model_version
    window_start, window_end = _window_bounds(settings)
    max_km = settings.dispersion_max_distance_km
    max_dist_m = max_km * 1000.0
    sigma_d = settings.dispersion_downwind_sigma_km
    sigma_c = settings.dispersion_crosswind_sigma_km
    min_wind = settings.dispersion_min_wind_speed_mps
    max_geo_writes = settings.dispersion_max_target_geographies

    metrics = {
        "detections_considered": 0,
        "wind_matches_found": 0,
        "exposures_written": 0,
        "skipped_no_wind": 0,
        "skipped_no_targets": 0,
    }

    geo_pairs: list[tuple[str, str]] = []
    if settings.smoke_risk_geographies in {"county", "both"}:
        geo_pairs.append(("county", "geo.counties"))
    if settings.smoke_risk_geographies in {"tract", "both"}:
        geo_pairs.append(("tract", "geo.tracts"))

    with connect(settings) as conn:
        _assert_tract_guard(conn, settings)

        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM analytics.smoke_dispersion_exposures
                WHERE window_start = %s AND window_end = %s AND model_version = %s
                """,
                (window_start, window_end, model_version),
            )
        conn.commit()

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT detection_id, longitude, latitude, acq_datetime, frp, brightness
                FROM normalized.fire_detections
                WHERE acq_datetime >= %s AND acq_datetime < %s
                ORDER BY acq_datetime DESC
                LIMIT 5000
                """,
                (window_start, window_end),
            )
            fires = list(cur.fetchall())

    metrics["detections_considered"] = len(fires)

    with connect(settings) as conn:
        for fire in fires:
            if metrics["exposures_written"] >= max_geo_writes:
                log.warning("dispersion_max_target_geographies cap reached", extra={"cap": max_geo_writes})
                break

            det_id = str(fire["detection_id"])
            flon = float(fire["longitude"])
            flat = float(fire["latitude"])

            wind_src, fallback_used = _resolve_wind_dispersion(conn, settings, fire)
            if not wind_src:
                metrics["skipped_no_wind"] += 1
                continue

            metrics["wind_matches_found"] += 1
            wind_from = float(wind_src["wind_direction_degrees"])
            w_speed_raw = wind_src.get("wind_speed_mps")
            w_speed = float(w_speed_raw) if w_speed_raw is not None else min_wind
            wid = wind_src.get("wind_observation_id")
            wcid = wind_src.get("weather_cell_id")
            dw = downwind_bearing(wind_from)
            if dw is None:
                metrics["skipped_no_wind"] += 1
                continue

            strength = source_strength_from_fire(
                float(fire["frp"]) if fire.get("frp") is not None else None,
                float(fire["brightness"]) if fire.get("brightness") is not None else None,
                settings.dispersion_source_strength_mode,
            )

            wrote_any = False
            for geo_type, rel in geo_pairs:
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
                        ORDER BY dist_km ASC
                        """,
                        (flon, flat, flon, flat, max_dist_m),
                    )
                    candidates = cur.fetchall()

                if not candidates:
                    continue

                for row in candidates:
                    if metrics["exposures_written"] >= max_geo_writes:
                        break

                    geoid = str(row["geoid"])
                    clon = float(row["clon"])
                    clat = float(row["clat"])
                    dist_km = float(row["dist_km"])
                    bear = bearing_degrees(flon, flat, clon, clat)
                    down_km, cross_km = wind_aligned_components(dist_km, bear, dw)

                    proxy = dispersion_concentration_proxy(
                        source_strength=strength,
                        downwind_km=down_km,
                        crosswind_km=cross_km,
                        wind_speed_mps=w_speed,
                        sigma_downwind_km=sigma_d,
                        sigma_crosswind_km=sigma_c,
                        min_wind_speed_mps=min_wind,
                    )
                    score = dispersion_score_from_proxy(proxy)
                    if score <= 0:
                        continue

                    downwind_component = (
                        gaussian_weight(max(down_km, 0.0) - sigma_d / 2.0, sigma_d) if down_km > 0 else 0.0
                    )
                    crosswind_component = gaussian_weight(cross_km, sigma_c)
                    eff_wind = max(w_speed, min_wind)
                    wind_component = min(1.0, eff_wind / 10.0)

                    explanation: dict[str, Any] = {
                        "source_strength": strength,
                        "source_strength_mode": settings.dispersion_source_strength_mode,
                        "downwind_component": downwind_component,
                        "crosswind_component": crosswind_component,
                        "wind_component": wind_component,
                        "weather_cell_id": wcid,
                        "wind_observation_id": wid,
                        "fallback_used": fallback_used,
                        "sigma_downwind_km": sigma_d,
                        "sigma_crosswind_km": sigma_c,
                        "disclaimer": "gaussian_v0 engineering proxy; not validated dispersion",
                    }
                    if settings.dispersion_write_debug_fields:
                        explanation["debug"] = {
                            "downwind_km": down_km,
                            "crosswind_km": cross_km,
                            "concentration_proxy": proxy,
                            "grid_resolution_km_config": settings.dispersion_grid_resolution_km,
                        }

                    with conn.cursor() as cur:
                        cur.execute(
                            UPSERT_DISPERSION,
                            (
                                model_version,
                                det_id,
                                geo_type,
                                geoid,
                                wcid,
                                wid,
                                window_start,
                                window_end,
                                dist_km,
                                down_km if down_km > 0 else None,
                                cross_km,
                                bear,
                                wind_from,
                                dw,
                                w_speed,
                                strength,
                                score,
                                proxy if proxy > 0 else None,
                                json.dumps(explanation),
                            ),
                        )
                    metrics["exposures_written"] += 1
                    wrote_any = True

            if not wrote_any:
                metrics["skipped_no_targets"] += 1

        conn.commit()

    print(json.dumps({"dispersion_metrics": metrics}, indent=2))
    log.info("dispersion_exposure_complete", extra=metrics)


if __name__ == "__main__":
    main()
