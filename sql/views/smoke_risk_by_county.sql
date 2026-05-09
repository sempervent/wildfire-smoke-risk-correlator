CREATE OR REPLACE VIEW analytics.smoke_risk_by_county AS
SELECT
    s.id,
    s.geoid,
    c.name AS county_name,
    c.statefp,
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
JOIN geo.counties c ON c.geoid = s.geoid
WHERE s.geography_type = 'county';
