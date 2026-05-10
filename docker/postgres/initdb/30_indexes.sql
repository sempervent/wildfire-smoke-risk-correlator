CREATE INDEX IF NOT EXISTS idx_geo_counties_geom ON geo.counties USING GiST (geom);
CREATE INDEX IF NOT EXISTS idx_geo_tracts_geom ON geo.tracts USING GiST (geom);
CREATE INDEX IF NOT EXISTS idx_geo_counties_statefp ON geo.counties (statefp);
CREATE INDEX IF NOT EXISTS idx_geo_tracts_statefp_countyfp ON geo.tracts (statefp, countyfp);

CREATE INDEX IF NOT EXISTS idx_raw_firms_fetched_at ON raw.firms_hotspots (fetched_at);
CREATE INDEX IF NOT EXISTS idx_raw_openaq_fetched_at ON raw.openaq_measurements (fetched_at);

CREATE INDEX IF NOT EXISTS idx_fire_detections_geom ON normalized.fire_detections USING GiST (geom);
CREATE INDEX IF NOT EXISTS idx_fire_detections_acq ON normalized.fire_detections (acq_datetime);
CREATE INDEX IF NOT EXISTS idx_fire_detections_county ON normalized.fire_detections (county_geoid);
CREATE INDEX IF NOT EXISTS idx_fire_detections_tract ON normalized.fire_detections (tract_geoid);

CREATE INDEX IF NOT EXISTS idx_aq_geom ON normalized.air_quality_measurements USING GiST (geom);
CREATE INDEX IF NOT EXISTS idx_aq_measured_at ON normalized.air_quality_measurements (measured_at);
CREATE INDEX IF NOT EXISTS idx_aq_county ON normalized.air_quality_measurements (county_geoid);
CREATE INDEX IF NOT EXISTS idx_aq_tract ON normalized.air_quality_measurements (tract_geoid);
CREATE INDEX IF NOT EXISTS idx_aq_parameter ON normalized.air_quality_measurements (parameter);

CREATE INDEX IF NOT EXISTS idx_smoke_risk_geoid ON analytics.smoke_risk_scores (geoid);
CREATE INDEX IF NOT EXISTS idx_smoke_risk_window ON analytics.smoke_risk_scores (window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_smoke_risk_geo_type ON analytics.smoke_risk_scores (geography_type);
