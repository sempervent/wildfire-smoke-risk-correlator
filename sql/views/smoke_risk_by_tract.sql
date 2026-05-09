CREATE OR REPLACE VIEW analytics.smoke_risk_by_tract AS
SELECT
    s.id,
    s.geoid,
    t.name AS tract_name,
    t.statefp,
    t.countyfp,
    s.window_start,
    s.window_end,
    s.fire_count,
    s.max_frp,
    s.avg_pm25,
    s.avg_pm10,
    s.risk_score,
    s.risk_band,
    s.computed_at
FROM analytics.smoke_risk_scores s
JOIN geo.tracts t ON t.geoid = s.geoid
WHERE s.geography_type = 'tract';
