SELECT
    measurement_id,
    parameter,
    value,
    unit,
    measured_at,
    latitude,
    longitude,
    county_geoid,
    tract_geoid
FROM normalized.air_quality_measurements
ORDER BY measured_at DESC
LIMIT 50;
