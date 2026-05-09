from __future__ import annotations

from wildfire_smoke.db.connection import connect


def assert_postgres_ready() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.execute("SELECT PostGIS_Version()")


def assert_geo_loaded() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM geo.counties")
            counties = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM geo.tracts")
            tracts = int(cur.fetchone()[0])
            if counties <= 0 or tracts <= 0:
                raise RuntimeError(f"geo tables appear empty (counties={counties}, tracts={tracts})")
