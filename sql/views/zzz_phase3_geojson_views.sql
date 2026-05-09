-- Phase 3: Grafana-oriented GeoJSON / lat-lon presentation views (canonical geometry remains geo.*).

CREATE OR REPLACE VIEW analytics.v_latest_smoke_risk_county_geojson AS
SELECT
  r.geoid,
  r.geography_name AS name,
  r.risk_score,
  r.risk_band,
  r.model_version,
  r.window_start,
  r.window_end,
  r.explanation,
  ST_AsGeoJSON(c.geom)::jsonb AS geojson,
  ST_AsGeoJSON(ST_SimplifyPreserveTopology(c.geom, 0.0003))::jsonb AS geojson_simplified,
  ST_Y(ST_Centroid(c.geom)) AS centroid_latitude,
  ST_X(ST_Centroid(c.geom)) AS centroid_longitude
FROM analytics.v_latest_smoke_risk_by_county r
JOIN geo.counties c ON c.geoid = r.geoid;

CREATE OR REPLACE VIEW analytics.v_latest_smoke_risk_tract_geojson AS
SELECT
  r.geoid,
  r.geography_name AS name,
  r.risk_score,
  r.risk_band,
  r.model_version,
  r.window_start,
  r.window_end,
  r.explanation,
  ST_AsGeoJSON(t.geom)::jsonb AS geojson,
  ST_AsGeoJSON(ST_SimplifyPreserveTopology(t.geom, 0.001))::jsonb AS geojson_simplified,
  ST_Y(ST_Centroid(t.geom)) AS centroid_latitude,
  ST_X(ST_Centroid(t.geom)) AS centroid_longitude
FROM analytics.v_latest_smoke_risk_by_tract r
JOIN geo.tracts t ON t.geoid = r.geoid;

CREATE OR REPLACE VIEW analytics.v_latest_fire_detections_geojson AS
SELECT
  f.detection_id,
  f.source,
  f.acq_datetime,
  f.county_geoid,
  f.tract_geoid,
  f.frp,
  f.confidence,
  f.latitude,
  f.longitude,
  f.inserted_at,
  ST_AsGeoJSON(f.geom)::jsonb AS geojson
FROM (
  SELECT *
  FROM normalized.fire_detections
  ORDER BY acq_datetime DESC NULLS LAST
  LIMIT 500
) f;

CREATE OR REPLACE VIEW analytics.v_latest_air_quality_geojson AS
SELECT
  a.measurement_id,
  a.provider,
  a.parameter,
  a.value,
  a.unit,
  a.measured_at,
  a.county_geoid,
  a.tract_geoid,
  a.latitude,
  a.longitude,
  a.inserted_at,
  ST_AsGeoJSON(a.geom)::jsonb AS geojson
FROM (
  SELECT *
  FROM normalized.air_quality_measurements
  ORDER BY measured_at DESC NULLS LAST
  LIMIT 500
) a;

COMMENT ON VIEW analytics.v_latest_smoke_risk_county_geojson IS
  'Presentation view: county polygons as GeoJSON for map layers. Canonical geometry: geo.counties.geom.';
COMMENT ON VIEW analytics.v_latest_smoke_risk_tract_geojson IS
  'Presentation view: tract polygons (simplified GeoJSON column) for dashboards. Canonical geometry: geo.tracts.geom.';
COMMENT ON VIEW analytics.v_latest_fire_detections_geojson IS
  'Presentation view: recent fire points as GeoJSON / lat/lon. Canonical storage: normalized.fire_detections.';
COMMENT ON VIEW analytics.v_latest_air_quality_geojson IS
  'Presentation view: recent AQ points as GeoJSON / lat/lon. Canonical storage: normalized.air_quality_measurements.';
