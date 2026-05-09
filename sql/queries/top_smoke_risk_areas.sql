SELECT
    geography_type,
    geoid,
    window_start,
    window_end,
    risk_score,
    risk_band,
    fire_count,
    max_frp,
    avg_pm25,
    avg_pm10
FROM analytics.smoke_risk_scores
WHERE computed_at >= now() - interval '7 days'
ORDER BY risk_score DESC
LIMIT 25;
