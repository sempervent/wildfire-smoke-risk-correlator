SELECT
    detection_id,
    source,
    latitude,
    longitude,
    acq_datetime,
    confidence,
    frp,
    county_geoid,
    tract_geoid
FROM normalized.fire_detections
ORDER BY acq_datetime DESC
LIMIT 50;
