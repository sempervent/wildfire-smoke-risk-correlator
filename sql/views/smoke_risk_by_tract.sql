DROP VIEW IF EXISTS analytics.smoke_risk_by_tract CASCADE;

CREATE VIEW analytics.smoke_risk_by_tract AS
SELECT
    s.id,
    s.geoid,
    t.name AS tract_name,
    t.statefp,
    t.countyfp,
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
JOIN geo.tracts t ON t.geoid = s.geoid
WHERE s.geography_type = 'tract';
