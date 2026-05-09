-- Phase 3: optional materialized snapshots for heavier dashboard queries (refresh after bulk loads).

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_latest_smoke_risk_county_geojson CASCADE;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_latest_smoke_risk_tract_geojson CASCADE;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_latest_smoke_risk_by_county CASCADE;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_latest_smoke_risk_by_tract CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_latest_smoke_risk_by_county AS
SELECT * FROM analytics.v_latest_smoke_risk_by_county;

CREATE MATERIALIZED VIEW analytics.mv_latest_smoke_risk_by_tract AS
SELECT * FROM analytics.v_latest_smoke_risk_by_tract;

CREATE MATERIALIZED VIEW analytics.mv_latest_smoke_risk_county_geojson AS
SELECT * FROM analytics.v_latest_smoke_risk_county_geojson;

CREATE MATERIALIZED VIEW analytics.mv_latest_smoke_risk_tract_geojson AS
SELECT * FROM analytics.v_latest_smoke_risk_tract_geojson;

CREATE UNIQUE INDEX IF NOT EXISTS mv_latest_smoke_risk_by_county_uid
  ON analytics.mv_latest_smoke_risk_by_county (geoid, model_version);

CREATE UNIQUE INDEX IF NOT EXISTS mv_latest_smoke_risk_by_tract_uid
  ON analytics.mv_latest_smoke_risk_by_tract (geoid, model_version);

CREATE UNIQUE INDEX IF NOT EXISTS mv_latest_smoke_risk_county_geojson_uid
  ON analytics.mv_latest_smoke_risk_county_geojson (geoid, model_version);

CREATE UNIQUE INDEX IF NOT EXISTS mv_latest_smoke_risk_tract_geojson_uid
  ON analytics.mv_latest_smoke_risk_tract_geojson (geoid, model_version);
