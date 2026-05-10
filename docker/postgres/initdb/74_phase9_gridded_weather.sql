-- Phase 9: bounded gridded weather ingest, cells, fire–weather matching.

CREATE TABLE IF NOT EXISTS raw.gridded_weather (
    id bigserial PRIMARY KEY,
    source text NOT NULL,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    grid_id text,
    valid_time timestamptz,
    payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS gridded_weather_fetched_at_idx ON raw.gridded_weather (fetched_at DESC);
CREATE INDEX IF NOT EXISTS gridded_weather_grid_idx ON raw.gridded_weather (grid_id);

COMMENT ON TABLE raw.gridded_weather IS 'Kafka-backed gridded weather payloads prior to normalization.';

CREATE TABLE IF NOT EXISTS normalized.weather_grid_cells (
    weather_cell_id text PRIMARY KEY,
    source text NOT NULL,
    grid_id text,
    valid_time timestamptz NOT NULL,
    forecast_time timestamptz,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    wind_speed_mps double precision,
    wind_direction_degrees double precision,
    temperature_c double precision,
    relative_humidity_percent double precision,
    geom geometry(Point, 4326) NOT NULL,
    county_geoid text REFERENCES geo.counties (geoid),
    tract_geoid text REFERENCES geo.tracts (geoid),
    inserted_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS weather_grid_cells_geom_gix ON normalized.weather_grid_cells USING GIST (geom);
CREATE INDEX IF NOT EXISTS weather_grid_cells_valid_time_idx ON normalized.weather_grid_cells (valid_time DESC);
CREATE INDEX IF NOT EXISTS weather_grid_cells_source_grid_idx ON normalized.weather_grid_cells (source, grid_id);

COMMENT ON TABLE normalized.weather_grid_cells IS 'Gridded weather samples (engineering use; not NWP analysis).';

CREATE TABLE IF NOT EXISTS analytics.fire_weather_matches (
    fire_weather_match_id bigserial PRIMARY KEY,
    detection_id text NOT NULL REFERENCES normalized.fire_detections (detection_id),
    weather_cell_id text NOT NULL REFERENCES normalized.weather_grid_cells (weather_cell_id),
    match_method text NOT NULL DEFAULT 'nearest_grid_cell',
    distance_km double precision,
    time_delta_minutes double precision,
    wind_speed_mps double precision,
    wind_direction_degrees double precision,
    temperature_c double precision,
    relative_humidity_percent double precision,
    matched_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fire_weather_matches_dedupe UNIQUE (detection_id, weather_cell_id, match_method)
);

CREATE INDEX IF NOT EXISTS fire_weather_matches_detection_idx ON analytics.fire_weather_matches (detection_id);
CREATE INDEX IF NOT EXISTS fire_weather_matches_cell_idx ON analytics.fire_weather_matches (weather_cell_id);
CREATE INDEX IF NOT EXISTS fire_weather_matches_matched_at_idx ON analytics.fire_weather_matches (matched_at DESC);

COMMENT ON TABLE analytics.fire_weather_matches IS 'Nearest grid-cell weather matched to fire detections (time + distance).';
