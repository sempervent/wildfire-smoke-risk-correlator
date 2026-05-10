# Database bootstrap

## Full Census load

Downloads **U.S. Census TIGER/Line** boundaries (default **Tennessee**; configurable multi-state or national county modes):

```bash
make db-bootstrap
```

Runs **`scripts/download_census_boundaries.sh`**, **`scripts/load_census_boundaries.sh`**, then **`scripts/bootstrap_db.sh`**.

**`bootstrap_db.sh`** applies **`sql/migrations/*.sql`** (migration **`013`** for canonical alert function **last**, after other migrations and after **`sql/views/`**).

Requires **`ogr2ogr`** — typically via Compose **`gdal-utils`** profile per **`docker-compose.yml`**.

## Minimal census (testing / CI)

Synthetic GeoJSON fixtures — **not** real TIGER boundaries:

```bash
make db-bootstrap-minimal
```

Refuses to overwrite populated **`geo.counties`** unless **`MINIMAL_CENSUS_REPLACE_ALL=1`** (destructive).

## After bootstrap

Create Kafka topics if not done:

```bash
make topics
```
