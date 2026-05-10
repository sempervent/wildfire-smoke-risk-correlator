"""Load synthetic county/tract GeoJSON for CI — not real Census operational data."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

from wildfire_smoke.db.connection import connect
from wildfire_smoke.logging import configure_logging
from wildfire_smoke.settings import Settings, repo_root

log = logging.getLogger(__name__)


def _geom_sql(geometry: dict) -> str:
    return json.dumps(geometry)


def load_geojson_counties(conn, path: Path) -> int:
    data = json.loads(path.read_text(encoding="utf-8"))
    n = 0
    with conn.cursor() as cur:
        for feat in data.get("features") or []:
            props = feat.get("properties") or {}
            geom = feat.get("geometry")
            if not geom:
                continue
            cur.execute(
                """
                INSERT INTO geo.counties (geoid, statefp, countyfp, name, aland, awater, geom)
                VALUES (%s, %s, %s, %s, %s, %s, ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))
                ON CONFLICT (geoid) DO UPDATE SET
                  statefp = EXCLUDED.statefp,
                  countyfp = EXCLUDED.countyfp,
                  name = EXCLUDED.name,
                  aland = EXCLUDED.aland,
                  awater = EXCLUDED.awater,
                  geom = EXCLUDED.geom
                """,
                (
                    str(props["geoid"]),
                    str(props["statefp"]),
                    str(props["countyfp"]),
                    str(props["name"]),
                    props.get("aland"),
                    props.get("awater"),
                    _geom_sql(geom),
                ),
            )
            n += 1
    return n


def load_geojson_tracts(conn, path: Path) -> int:
    data = json.loads(path.read_text(encoding="utf-8"))
    n = 0
    with conn.cursor() as cur:
        for feat in data.get("features") or []:
            props = feat.get("properties") or {}
            geom = feat.get("geometry")
            if not geom:
                continue
            cur.execute(
                """
                INSERT INTO geo.tracts (geoid, statefp, countyfp, tractce, name, aland, awater, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))
                ON CONFLICT (geoid) DO UPDATE SET
                  statefp = EXCLUDED.statefp,
                  countyfp = EXCLUDED.countyfp,
                  tractce = EXCLUDED.tractce,
                  name = EXCLUDED.name,
                  aland = EXCLUDED.aland,
                  awater = EXCLUDED.awater,
                  geom = EXCLUDED.geom
                """,
                (
                    str(props["geoid"]),
                    str(props["statefp"]),
                    str(props["countyfp"]),
                    str(props["tractce"]),
                    str(props["name"]),
                    props.get("aland"),
                    props.get("awater"),
                    _geom_sql(geom),
                ),
            )
            n += 1
    return n


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    root = repo_root()
    counties_path = Path(
        os.environ.get(
            "MINIMAL_CENSUS_COUNTIES_GEOJSON",
            str(root / "tests/fixtures/census_minimal_counties.geojson"),
        )
    )
    tracts_path = Path(
        os.environ.get(
            "MINIMAL_CENSUS_TRACTS_GEOJSON",
            str(root / "tests/fixtures/census_minimal_tracts.geojson"),
        )
    )
    replace_all = os.environ.get("MINIMAL_CENSUS_REPLACE_ALL", "0").strip().lower() in {"1", "true", "yes"}

    if not counties_path.is_file() or not tracts_path.is_file():
        print("Minimal census GeoJSON fixtures missing.", file=sys.stderr)
        raise SystemExit(1)

    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::bigint FROM geo.counties")
            existing = int(cur.fetchone()[0])

        if existing > 0 and not replace_all:
            print(
                "geo.counties already has rows; refusing to load minimal fixtures "
                "(set MINIMAL_CENSUS_REPLACE_ALL=1 for CI destructive reload).",
                file=sys.stderr,
            )
            raise SystemExit(1)

        if replace_all:
            log.warning("minimal_census_truncating_geo_tables")
            with conn.cursor() as cur:
                cur.execute("TRUNCATE geo.tracts CASCADE")
                cur.execute("TRUNCATE geo.counties CASCADE")

        nc = load_geojson_counties(conn, counties_path)
        nt = load_geojson_tracts(conn, tracts_path)
        conn.commit()

    print(json.dumps({"counties_upserted": nc, "tracts_upserted": nt}, indent=2))
    log.info("minimal_census_load_complete", extra={"counties": nc, "tracts": nt})


if __name__ == "__main__":
    main()
