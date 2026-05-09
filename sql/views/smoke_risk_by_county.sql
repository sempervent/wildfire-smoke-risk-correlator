DROP VIEW IF EXISTS analytics.smoke_risk_by_county CASCADE;

CREATE VIEW analytics.smoke_risk_by_county AS
SELECT
    s.id,
    s.geoid,
    c.name AS county_name,
    c.statefp,
    s.window_start,
    s.window_end,
    s.model_version,
    s.fire_count,
    s.nearby_fire_count,
    s.nearest_fire_km,
    s.max_frp,
    s.avg_pm25,
    s.avg_pm10,
    s.aq_observation_count,
    s.newest_aq_observed_at,
    s.newest_fire_observed_at,
    s.risk_score,
    s.risk_band,
    s.explanation,
    s.computed_at
FROM analytics.smoke_risk_scores s
JOIN geo.counties c ON c.geoid = s.geoid
WHERE s.geography_type = 'county';
