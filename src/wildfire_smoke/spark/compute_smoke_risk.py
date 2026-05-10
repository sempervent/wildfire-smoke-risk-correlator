from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from psycopg.rows import dict_row
from psycopg.types.json import Json

from wildfire_smoke.db.connection import connect
from wildfire_smoke.risk import (
    compute_risk_score_fields,
    compute_risk_score_v2_fields,
    compute_risk_score_v3_fields,
    compute_risk_score_v4_fields,
    compute_risk_score_v5_fields,
)
from wildfire_smoke.settings import Settings, kafka_topics

log = logging.getLogger(__name__)

COUNTY_V1_SQL = """
WITH fires AS (
  SELECT county_geoid AS geoid,
         COUNT(*)::int AS fire_count,
         MAX(frp) AS max_frp,
         MAX(acq_datetime) AS newest_fire_observed_at
  FROM normalized.fire_detections
  WHERE acq_datetime >= %s AND acq_datetime < %s AND county_geoid IS NOT NULL
  GROUP BY county_geoid
),
aq AS (
  SELECT county_geoid AS geoid,
         AVG(value) FILTER (WHERE parameter = 'pm25') AS avg_pm25,
         AVG(value) FILTER (WHERE parameter = 'pm10') AS avg_pm10,
         COUNT(*)::int AS aq_observation_count,
         MAX(measured_at) AS newest_aq_observed_at
  FROM normalized.air_quality_measurements
  WHERE measured_at >= %s AND measured_at < %s AND county_geoid IS NOT NULL
  GROUP BY county_geoid
)
SELECT COALESCE(f.geoid, a.geoid) AS geoid,
       COALESCE(f.fire_count, 0)::int AS fire_count,
       f.max_frp,
       a.avg_pm25,
       a.avg_pm10,
       COALESCE(a.aq_observation_count, 0)::int AS aq_observation_count,
       a.newest_aq_observed_at,
       f.newest_fire_observed_at
FROM fires f
FULL OUTER JOIN aq a ON f.geoid = a.geoid
WHERE COALESCE(f.geoid, a.geoid) IS NOT NULL;
"""

TRACT_V1_SQL = """
WITH fires AS (
  SELECT tract_geoid AS geoid,
         COUNT(*)::int AS fire_count,
         MAX(frp) AS max_frp,
         MAX(acq_datetime) AS newest_fire_observed_at
  FROM normalized.fire_detections
  WHERE acq_datetime >= %s AND acq_datetime < %s AND tract_geoid IS NOT NULL
  GROUP BY tract_geoid
),
aq AS (
  SELECT tract_geoid AS geoid,
         AVG(value) FILTER (WHERE parameter = 'pm25') AS avg_pm25,
         AVG(value) FILTER (WHERE parameter = 'pm10') AS avg_pm10,
         COUNT(*)::int AS aq_observation_count,
         MAX(measured_at) AS newest_aq_observed_at
  FROM normalized.air_quality_measurements
  WHERE measured_at >= %s AND measured_at < %s AND tract_geoid IS NOT NULL
  GROUP BY tract_geoid
)
SELECT COALESCE(f.geoid, a.geoid) AS geoid,
       COALESCE(f.fire_count, 0)::int AS fire_count,
       f.max_frp,
       a.avg_pm25,
       a.avg_pm10,
       COALESCE(a.aq_observation_count, 0)::int AS aq_observation_count,
       a.newest_aq_observed_at,
       f.newest_fire_observed_at
FROM fires f
FULL OUTER JOIN aq a ON f.geoid = a.geoid
WHERE COALESCE(f.geoid, a.geoid) IS NOT NULL;
"""

COUNTY_V2_SQL = """
SELECT
  c.geoid,
  (
    SELECT COUNT(*)::int FROM normalized.fire_detections f
    WHERE f.acq_datetime >= %s AND f.acq_datetime < %s
      AND ST_Contains(c.geom, f.geom)
  ) AS fire_inside_count,
  (
    SELECT COUNT(*)::int FROM normalized.fire_detections f
    WHERE f.acq_datetime >= %s AND f.acq_datetime < %s
      AND NOT ST_Contains(c.geom, f.geom)
      AND ST_DWithin(f.geom::geography, c.geom::geography, %s)
  ) AS nearby_fire_count,
  (
    SELECT MAX(f.frp) FROM normalized.fire_detections f
    WHERE f.acq_datetime >= %s AND f.acq_datetime < %s
      AND (
        ST_Contains(c.geom, f.geom)
        OR ST_DWithin(f.geom::geography, c.geom::geography, %s)
      )
  ) AS max_frp,
  (
    SELECT MAX(f.acq_datetime) FROM normalized.fire_detections f
    WHERE f.acq_datetime >= %s AND f.acq_datetime < %s
      AND (
        ST_Contains(c.geom, f.geom)
        OR ST_DWithin(f.geom::geography, c.geom::geography, %s)
      )
  ) AS newest_fire_observed_at,
  (
    SELECT MIN(ST_Distance(ST_Centroid(c.geom)::geography, f.geom::geography)) / 1000.0
    FROM normalized.fire_detections f
    WHERE f.acq_datetime >= %s AND f.acq_datetime < %s
      AND (
        ST_Contains(c.geom, f.geom)
        OR ST_DWithin(f.geom::geography, c.geom::geography, %s)
      )
  ) AS nearest_fire_km,
  (
    SELECT COUNT(*)::int FROM normalized.air_quality_measurements a
    WHERE a.measured_at >= %s AND a.measured_at < %s
      AND a.county_geoid = c.geoid
  ) AS aq_observation_count,
  (
    SELECT MAX(a.measured_at) FROM normalized.air_quality_measurements a
    WHERE a.measured_at >= %s AND a.measured_at < %s
      AND a.county_geoid = c.geoid
  ) AS newest_aq_observed_at,
  (
    SELECT AVG(a.value) FILTER (WHERE a.parameter = 'pm25')
    FROM normalized.air_quality_measurements a
    WHERE a.measured_at >= %s AND a.measured_at < %s
      AND a.county_geoid = c.geoid
  ) AS avg_pm25,
  (
    SELECT AVG(a.value) FILTER (WHERE a.parameter = 'pm10')
    FROM normalized.air_quality_measurements a
    WHERE a.measured_at >= %s AND a.measured_at < %s
      AND a.county_geoid = c.geoid
  ) AS avg_pm10
FROM geo.counties c;
"""

TRACT_V2_SQL = """
SELECT
  t.geoid,
  (
    SELECT COUNT(*)::int FROM normalized.fire_detections f
    WHERE f.acq_datetime >= %s AND f.acq_datetime < %s
      AND ST_Contains(t.geom, f.geom)
  ) AS fire_inside_count,
  (
    SELECT COUNT(*)::int FROM normalized.fire_detections f
    WHERE f.acq_datetime >= %s AND f.acq_datetime < %s
      AND NOT ST_Contains(t.geom, f.geom)
      AND ST_DWithin(f.geom::geography, t.geom::geography, %s)
  ) AS nearby_fire_count,
  (
    SELECT MAX(f.frp) FROM normalized.fire_detections f
    WHERE f.acq_datetime >= %s AND f.acq_datetime < %s
      AND (
        ST_Contains(t.geom, f.geom)
        OR ST_DWithin(f.geom::geography, t.geom::geography, %s)
      )
  ) AS max_frp,
  (
    SELECT MAX(f.acq_datetime) FROM normalized.fire_detections f
    WHERE f.acq_datetime >= %s AND f.acq_datetime < %s
      AND (
        ST_Contains(t.geom, f.geom)
        OR ST_DWithin(f.geom::geography, t.geom::geography, %s)
      )
  ) AS newest_fire_observed_at,
  (
    SELECT MIN(ST_Distance(ST_Centroid(t.geom)::geography, f.geom::geography)) / 1000.0
    FROM normalized.fire_detections f
    WHERE f.acq_datetime >= %s AND f.acq_datetime < %s
      AND (
        ST_Contains(t.geom, f.geom)
        OR ST_DWithin(f.geom::geography, t.geom::geography, %s)
      )
  ) AS nearest_fire_km,
  (
    SELECT COUNT(*)::int FROM normalized.air_quality_measurements a
    WHERE a.measured_at >= %s AND a.measured_at < %s
      AND a.tract_geoid = t.geoid
  ) AS aq_observation_count,
  (
    SELECT MAX(a.measured_at) FROM normalized.air_quality_measurements a
    WHERE a.measured_at >= %s AND a.measured_at < %s
      AND a.tract_geoid = t.geoid
  ) AS newest_aq_observed_at,
  (
    SELECT AVG(a.value) FILTER (WHERE a.parameter = 'pm25')
    FROM normalized.air_quality_measurements a
    WHERE a.measured_at >= %s AND a.measured_at < %s
      AND a.tract_geoid = t.geoid
  ) AS avg_pm25,
  (
    SELECT AVG(a.value) FILTER (WHERE a.parameter = 'pm10')
    FROM normalized.air_quality_measurements a
    WHERE a.measured_at >= %s AND a.measured_at < %s
      AND a.tract_geoid = t.geoid
  ) AS avg_pm10
FROM geo.tracts t;
"""


def _window_bounds(settings: Settings) -> tuple[datetime, datetime]:
    window_end = datetime.now(timezone.utc).replace(microsecond=0)
    window_start = (window_end - timedelta(hours=settings.smoke_risk_lookback_hours)).replace(microsecond=0)
    return window_start, window_end


def _v2_sql_params(window_start: datetime, window_end: datetime, radius_m: float) -> tuple:
    ws, we = window_start, window_end
    return (
        ws,
        we,
        ws,
        we,
        radius_m,
        ws,
        we,
        radius_m,
        ws,
        we,
        radius_m,
        ws,
        we,
        radius_m,
        ws,
        we,
        ws,
        we,
        ws,
        we,
        ws,
        we,
    )


def _dispersion_max_by_geography(
    conn, window_start: datetime, window_end: datetime, dispersion_model_version: str
) -> dict[tuple[str, str], tuple[float, int]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT geography_type, geoid,
                   COALESCE(MAX(dispersion_score), 0)::double precision AS max_dispersion,
                   COUNT(DISTINCT detection_id)::int AS det_count
            FROM analytics.smoke_dispersion_exposures
            WHERE window_start = %s AND window_end = %s AND model_version = %s
            GROUP BY geography_type, geoid
            """,
            (window_start, window_end, dispersion_model_version),
        )
        rows = cur.fetchall()
    out: dict[tuple[str, str], tuple[float, int]] = {}
    for r in rows:
        out[(str(r["geography_type"]), str(r["geoid"]))] = (float(r["max_dispersion"]), int(r["det_count"]))
    return out


def _plume_max_by_geography_model(
    conn, window_start: datetime, window_end: datetime, plume_model_version: str
) -> dict[tuple[str, str], tuple[float, int]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT geography_type, geoid,
                   COALESCE(MAX(exposure_score), 0)::double precision AS max_plume,
                   COUNT(DISTINCT detection_id)::int AS det_count
            FROM analytics.smoke_plume_exposures
            WHERE window_start = %s AND window_end = %s AND model_version = %s
            GROUP BY geography_type, geoid
            """,
            (window_start, window_end, plume_model_version),
        )
        rows = cur.fetchall()
    out: dict[tuple[str, str], tuple[float, int]] = {}
    for r in rows:
        out[(str(r["geography_type"]), str(r["geoid"]))] = (float(r["max_plume"]), int(r["det_count"]))
    return out


def _grid_weather_stats_by_geography(
    conn, window_start: datetime, window_end: datetime
) -> dict[tuple[str, str], tuple[float | None, int]]:
    out: dict[tuple[str, str], tuple[float | None, int]] = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT 'county'::text AS geography_type, county_geoid AS geoid,
                   AVG(relative_humidity_percent)::double precision AS avg_rh,
                   COUNT(*)::int AS ncells
            FROM normalized.weather_grid_cells
            WHERE county_geoid IS NOT NULL
              AND valid_time >= %s AND valid_time < %s
            GROUP BY county_geoid
            """,
            (window_start, window_end),
        )
        for r in cur.fetchall():
            out[("county", str(r["geoid"]))] = (
                float(r["avg_rh"]) if r.get("avg_rh") is not None else None,
                int(r["ncells"]),
            )
        cur.execute(
            """
            SELECT 'tract'::text AS geography_type, tract_geoid AS geoid,
                   AVG(relative_humidity_percent)::double precision AS avg_rh,
                   COUNT(*)::int AS ncells
            FROM normalized.weather_grid_cells
            WHERE tract_geoid IS NOT NULL
              AND valid_time >= %s AND valid_time < %s
            GROUP BY tract_geoid
            """,
            (window_start, window_end),
        )
        for r in cur.fetchall():
            out[("tract", str(r["geoid"]))] = (
                float(r["avg_rh"]) if r.get("avg_rh") is not None else None,
                int(r["ncells"]),
            )
    return out


def _v4_plume_selection(
    settings: Settings,
    grid_plumes: dict[tuple[str, str], tuple[float, int]],
    station_plumes: dict[tuple[str, str], tuple[float, int]],
) -> tuple[dict[tuple[str, str], tuple[float, int]], dict[tuple[str, str], bool]]:
    """Pick per-geography plume score for v4; value tuple is (max_score, det_count)."""

    keys = set(grid_plumes.keys()) | set(station_plumes.keys())
    chosen: dict[tuple[str, str], tuple[float, int]] = {}
    fallback: dict[tuple[str, str], bool] = {}
    for k in keys:
        g_max, g_det = grid_plumes.get(k, (0.0, 0))
        s_max, s_det = station_plumes.get(k, (0.0, 0))
        if settings.plume_model_version == "wind_grid_v2":
            if g_max > 0:
                chosen[k] = (g_max, g_det)
                fallback[k] = False
            elif settings.plume_grid_fallback_to_station and s_max > 0:
                chosen[k] = (s_max, s_det)
                fallback[k] = True
            else:
                chosen[k] = (0.0, 0)
                fallback[k] = False
        else:
            chosen[k] = (s_max, s_det)
            fallback[k] = False
    return chosen, fallback


def _has_activity_v2(row: dict) -> bool:
    fi = int(row.get("fire_inside_count") or 0)
    nf = int(row.get("nearby_fire_count") or 0)
    aq = int(row.get("aq_observation_count") or 0)
    return fi > 0 or nf > 0 or aq > 0 or row.get("avg_pm25") is not None or row.get("avg_pm10") is not None


UPSERT_SQL = """
INSERT INTO analytics.smoke_risk_scores (
  geography_type, geoid, window_start, window_end, model_version,
  explanation,
  fire_count, nearby_fire_count, nearest_fire_km,
  max_frp, avg_pm25, avg_pm10,
  aq_observation_count, newest_aq_observed_at, newest_fire_observed_at,
  risk_score, risk_band
) VALUES (
  %s, %s, %s, %s, %s,
  %s::jsonb,
  %s, %s, %s,
  %s, %s, %s,
  %s, %s, %s,
  %s, %s
)
ON CONFLICT (geography_type, geoid, window_start, window_end, model_version)
DO UPDATE SET
  explanation = EXCLUDED.explanation,
  fire_count = EXCLUDED.fire_count,
  nearby_fire_count = EXCLUDED.nearby_fire_count,
  nearest_fire_km = EXCLUDED.nearest_fire_km,
  max_frp = EXCLUDED.max_frp,
  avg_pm25 = EXCLUDED.avg_pm25,
  avg_pm10 = EXCLUDED.avg_pm10,
  aq_observation_count = EXCLUDED.aq_observation_count,
  newest_aq_observed_at = EXCLUDED.newest_aq_observed_at,
  newest_fire_observed_at = EXCLUDED.newest_fire_observed_at,
  risk_score = EXCLUDED.risk_score,
  risk_band = EXCLUDED.risk_band,
  computed_at = now();
"""


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    topics = kafka_topics()

    window_start, window_end = _window_bounds(settings)
    model_version = settings.smoke_risk_model_version
    radius_m = float(settings.smoke_risk_nearby_km) * 1000.0

    geographies = settings.smoke_risk_geographies
    if geographies not in {"county", "tract", "both"}:
        raise ValueError("SMOKE_RISK_GEOGRAPHIES must be one of: county, tract, both")

    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM analytics.smoke_risk_scores
                WHERE window_start = %s AND window_end = %s AND model_version = %s
                """,
                (window_start, window_end, model_version),
            )
        conn.commit()

    rows_out: list[dict] = []

    with connect(settings) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            if model_version == "v1":
                if geographies in {"county", "both"}:
                    cur.execute(COUNTY_V1_SQL, (window_start, window_end, window_start, window_end))
                    for r in cur.fetchall():
                        rows_out.append({**dict(r), "geography_type": "county"})
                if geographies in {"tract", "both"}:
                    cur.execute(TRACT_V1_SQL, (window_start, window_end, window_start, window_end))
                    for r in cur.fetchall():
                        rows_out.append({**dict(r), "geography_type": "tract"})
            elif model_version in {"v2", "v3", "v4", "v5"}:
                params = _v2_sql_params(window_start, window_end, radius_m)
                if geographies in {"county", "both"}:
                    cur.execute(COUNTY_V2_SQL, params)
                    for r in cur.fetchall():
                        row = dict(r)
                        if not _has_activity_v2(row):
                            continue
                        rows_out.append({**row, "geography_type": "county"})
                if geographies in {"tract", "both"}:
                    cur.execute(TRACT_V2_SQL, params)
                    for r in cur.fetchall():
                        row = dict(r)
                        if not _has_activity_v2(row):
                            continue
                        rows_out.append({**row, "geography_type": "tract"})
            else:
                raise ValueError(f"Unsupported SMOKE_RISK_MODEL_VERSION: {model_version!r}")

    kafka_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in kafka_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    plume_by_geo: dict[tuple[str, str], tuple[float, int]] = {}
    plume_by_geo_v4: dict[tuple[str, str], tuple[float, int]] = {}
    plume_fallback_v4: dict[tuple[str, str], bool] = {}
    grid_stats: dict[tuple[str, str], tuple[float | None, int]] = {}
    dispersion_by_geo: dict[tuple[str, str], tuple[float, int]] = {}
    if model_version == "v3":
        with connect(settings) as conn:
            plume_by_geo = _plume_max_by_geography_model(conn, window_start, window_end, "wind_v1")
    elif model_version == "v4":
        with connect(settings) as conn:
            grid_plumes = _plume_max_by_geography_model(conn, window_start, window_end, "wind_grid_v2")
            station_plumes = _plume_max_by_geography_model(conn, window_start, window_end, "wind_v1")
            plume_by_geo_v4, plume_fallback_v4 = _v4_plume_selection(settings, grid_plumes, station_plumes)
            grid_stats = _grid_weather_stats_by_geography(conn, window_start, window_end)
    elif model_version == "v5":
        with connect(settings) as conn:
            grid_plumes = _plume_max_by_geography_model(conn, window_start, window_end, "wind_grid_v2")
            station_plumes = _plume_max_by_geography_model(conn, window_start, window_end, "wind_v1")
            plume_by_geo_v4, plume_fallback_v4 = _v4_plume_selection(settings, grid_plumes, station_plumes)
            grid_stats = _grid_weather_stats_by_geography(conn, window_start, window_end)
            dispersion_by_geo = _dispersion_max_by_geography(
                conn, window_start, window_end, settings.dispersion_model_version
            )

    with connect(settings) as conn:
        with conn.cursor() as cur:
            for r in rows_out:
                geo_type = str(r["geography_type"])
                geoid = str(r["geoid"])

                if model_version == "v1":
                    fire_count = int(r["fire_count"] or 0)
                    max_frp = float(r["max_frp"]) if r.get("max_frp") is not None else None
                    avg_pm25 = float(r["avg_pm25"]) if r.get("avg_pm25") is not None else None
                    avg_pm10 = float(r["avg_pm10"]) if r.get("avg_pm10") is not None else None
                    risk_score, risk_band_val = compute_risk_score_fields(fire_count, max_frp, avg_pm25, avg_pm10)
                    explanation = {
                        "model_version": "v1",
                        "inputs": {
                            "fire_count": fire_count,
                            "max_frp": max_frp,
                            "avg_pm25": avg_pm25,
                            "avg_pm10": avg_pm10,
                            "window_end": window_end.isoformat(),
                        },
                    }
                    nearby_fire_count = 0
                    nearest_fire_km = None
                    aq_observation_count = int(r.get("aq_observation_count") or 0)
                    newest_aq = r.get("newest_aq_observed_at")
                    newest_fire = r.get("newest_fire_observed_at")
                elif model_version == "v2":
                    fire_inside = int(r["fire_inside_count"] or 0)
                    nearby_fire_count = int(r["nearby_fire_count"] or 0)
                    max_frp = float(r["max_frp"]) if r.get("max_frp") is not None else None
                    avg_pm25 = float(r["avg_pm25"]) if r.get("avg_pm25") is not None else None
                    avg_pm10 = float(r["avg_pm10"]) if r.get("avg_pm10") is not None else None
                    newest_fire = r.get("newest_fire_observed_at")
                    risk_score, risk_band_val, explanation = compute_risk_score_v2_fields(
                        fire_inside_count=fire_inside,
                        nearby_fire_count=nearby_fire_count,
                        max_frp=max_frp,
                        avg_pm25=avg_pm25,
                        avg_pm10=avg_pm10,
                        newest_fire_observed_at=newest_fire,
                        window_end=window_end,
                    )
                    fire_count = fire_inside
                    nearest_fire_km = float(r["nearest_fire_km"]) if r.get("nearest_fire_km") is not None else None
                    aq_observation_count = int(r.get("aq_observation_count") or 0)
                    newest_aq = r.get("newest_aq_observed_at")
                elif model_version == "v3":
                    fire_inside = int(r["fire_inside_count"] or 0)
                    nearby_fire_count = int(r["nearby_fire_count"] or 0)
                    max_frp = float(r["max_frp"]) if r.get("max_frp") is not None else None
                    avg_pm25 = float(r["avg_pm25"]) if r.get("avg_pm25") is not None else None
                    avg_pm10 = float(r["avg_pm10"]) if r.get("avg_pm10") is not None else None
                    newest_fire = r.get("newest_fire_observed_at")
                    max_plume, det_plume = plume_by_geo.get((geo_type, geoid), (0.0, 0))
                    risk_score, risk_band_val, explanation = compute_risk_score_v3_fields(
                        fire_inside_count=fire_inside,
                        nearby_fire_count=nearby_fire_count,
                        max_frp=max_frp,
                        avg_pm25=avg_pm25,
                        avg_pm10=avg_pm10,
                        newest_fire_observed_at=newest_fire,
                        window_end=window_end,
                        max_plume_exposure_score=max_plume,
                        plume_detection_count=det_plume,
                        wind_model_version="wind_v1",
                    )
                    fire_count = fire_inside
                    nearest_fire_km = float(r["nearest_fire_km"]) if r.get("nearest_fire_km") is not None else None
                    aq_observation_count = int(r.get("aq_observation_count") or 0)
                    newest_aq = r.get("newest_aq_observed_at")
                elif model_version == "v4":
                    fire_inside = int(r["fire_inside_count"] or 0)
                    nearby_fire_count = int(r["nearby_fire_count"] or 0)
                    max_frp = float(r["max_frp"]) if r.get("max_frp") is not None else None
                    avg_pm25 = float(r["avg_pm25"]) if r.get("avg_pm25") is not None else None
                    avg_pm10 = float(r["avg_pm10"]) if r.get("avg_pm10") is not None else None
                    newest_fire = r.get("newest_fire_observed_at")
                    max_plume, det_plume = plume_by_geo_v4.get((geo_type, geoid), (0.0, 0))
                    plume_fb = plume_fallback_v4.get((geo_type, geoid), False)
                    avg_rh, ngrid = grid_stats.get((geo_type, geoid), (None, 0))
                    risk_score, risk_band_val, explanation = compute_risk_score_v4_fields(
                        fire_inside_count=fire_inside,
                        nearby_fire_count=nearby_fire_count,
                        max_frp=max_frp,
                        avg_pm25=avg_pm25,
                        avg_pm10=avg_pm10,
                        newest_fire_observed_at=newest_fire,
                        window_end=window_end,
                        max_plume_exposure_score=max_plume,
                        plume_detection_count=det_plume,
                        plume_model_version=settings.plume_model_version,
                        avg_relative_humidity_percent=avg_rh,
                        grid_weather_observation_count=ngrid,
                        plume_fallback_used=plume_fb,
                    )
                    fire_count = fire_inside
                    nearest_fire_km = float(r["nearest_fire_km"]) if r.get("nearest_fire_km") is not None else None
                    aq_observation_count = int(r.get("aq_observation_count") or 0)
                    newest_aq = r.get("newest_aq_observed_at")
                elif model_version == "v5":
                    fire_inside = int(r["fire_inside_count"] or 0)
                    nearby_fire_count = int(r["nearby_fire_count"] or 0)
                    max_frp = float(r["max_frp"]) if r.get("max_frp") is not None else None
                    avg_pm25 = float(r["avg_pm25"]) if r.get("avg_pm25") is not None else None
                    avg_pm10 = float(r["avg_pm10"]) if r.get("avg_pm10") is not None else None
                    newest_fire = r.get("newest_fire_observed_at")
                    max_plume, det_plume = plume_by_geo_v4.get((geo_type, geoid), (0.0, 0))
                    plume_fb = plume_fallback_v4.get((geo_type, geoid), False)
                    avg_rh, ngrid = grid_stats.get((geo_type, geoid), (None, 0))
                    max_disp, disp_det = dispersion_by_geo.get((geo_type, geoid), (0.0, 0))
                    risk_score, risk_band_val, explanation = compute_risk_score_v5_fields(
                        fire_inside_count=fire_inside,
                        nearby_fire_count=nearby_fire_count,
                        max_frp=max_frp,
                        avg_pm25=avg_pm25,
                        avg_pm10=avg_pm10,
                        newest_fire_observed_at=newest_fire,
                        window_end=window_end,
                        max_plume_exposure_score=max_plume,
                        plume_detection_count=det_plume,
                        plume_model_version=settings.plume_model_version,
                        max_dispersion_score=max_disp,
                        dispersion_detection_count=disp_det,
                        dispersion_model_version=settings.dispersion_model_version,
                        avg_relative_humidity_percent=avg_rh,
                        grid_weather_observation_count=ngrid,
                        plume_fallback_used=plume_fb,
                    )
                    fire_count = fire_inside
                    nearest_fire_km = float(r["nearest_fire_km"]) if r.get("nearest_fire_km") is not None else None
                    aq_observation_count = int(r.get("aq_observation_count") or 0)
                    newest_aq = r.get("newest_aq_observed_at")
                else:
                    raise ValueError(f"Unsupported model branch: {model_version!r}")

                cur.execute(
                    UPSERT_SQL,
                    (
                        geo_type,
                        geoid,
                        window_start,
                        window_end,
                        model_version,
                        Json(explanation),
                        fire_count,
                        nearby_fire_count,
                        nearest_fire_km,
                        max_frp,
                        avg_pm25,
                        avg_pm10,
                        aq_observation_count,
                        newest_aq,
                        newest_fire,
                        risk_score,
                        risk_band_val,
                    ),
                )

                payload = {
                    "geography_type": geo_type,
                    "geoid": geoid,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "model_version": model_version,
                    "fire_count": fire_count,
                    "nearby_fire_count": nearby_fire_count,
                    "nearest_fire_km": nearest_fire_km,
                    "max_frp": max_frp,
                    "avg_pm25": avg_pm25,
                    "avg_pm10": avg_pm10,
                    "aq_observation_count": aq_observation_count,
                    "newest_aq_observed_at": newest_aq.isoformat() if newest_aq is not None else None,
                    "newest_fire_observed_at": newest_fire.isoformat() if newest_fire is not None else None,
                    "risk_score": risk_score,
                    "risk_band": risk_band_val,
                    "explanation": explanation,
                }
                producer.send(topics["smoke_risk_topic"], value=payload)

        conn.commit()

    producer.flush()
    producer.close()

    log.info(
        "smoke_risk_complete",
        extra={
            "model_version": model_version,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "rows": len(rows_out),
            "geographies": geographies,
            "lookback_hours": settings.smoke_risk_lookback_hours,
            "nearby_km": settings.smoke_risk_nearby_km,
        },
    )


if __name__ == "__main__":
    main()
