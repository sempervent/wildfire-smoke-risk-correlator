from __future__ import annotations

from psycopg import Connection


def associate_fire_points(conn: Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE normalized.fire_detections f
            SET county_geoid = c.geoid
            FROM geo.counties c
            WHERE ST_Intersects(c.geom, f.geom);
            """
        )
        cur.execute(
            """
            UPDATE normalized.fire_detections f
            SET tract_geoid = t.geoid
            FROM geo.tracts t
            WHERE ST_Intersects(t.geom, f.geom);
            """
        )


def associate_wind_points(conn: Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE normalized.wind_observations w
            SET county_geoid = c.geoid
            FROM geo.counties c
            WHERE ST_Intersects(c.geom, w.geom);
            """
        )
        cur.execute(
            """
            UPDATE normalized.wind_observations w
            SET tract_geoid = t.geoid
            FROM geo.tracts t
            WHERE ST_Intersects(t.geom, w.geom);
            """
        )


def associate_air_quality_points(conn: Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE normalized.air_quality_measurements a
            SET county_geoid = c.geoid
            FROM geo.counties c
            WHERE ST_Intersects(c.geom, a.geom);
            """
        )
        cur.execute(
            """
            UPDATE normalized.air_quality_measurements a
            SET tract_geoid = t.geoid
            FROM geo.tracts t
            WHERE ST_Intersects(t.geom, a.geom);
            """
        )
