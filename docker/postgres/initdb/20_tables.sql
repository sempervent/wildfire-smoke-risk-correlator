CREATE TABLE IF NOT EXISTS geo.counties (
    geoid text PRIMARY KEY,
    statefp text NOT NULL,
    countyfp text NOT NULL,
    name text NOT NULL,
    aland double precision,
    awater double precision,
    geom geometry(MultiPolygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS geo.tracts (
    geoid text PRIMARY KEY,
    statefp text NOT NULL,
    countyfp text NOT NULL,
    tractce text NOT NULL,
    name text NOT NULL,
    aland double precision,
    awater double precision,
    geom geometry(MultiPolygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.firms_hotspots (
    id bigserial PRIMARY KEY,
    source text NOT NULL,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS normalized.fire_detections (
    detection_id text PRIMARY KEY,
    source text NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    acq_datetime timestamptz NOT NULL,
    confidence text,
    brightness double precision,
    frp double precision,
    daynight text,
    geom geometry(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
    ) STORED,
    county_geoid text REFERENCES geo.counties (geoid),
    tract_geoid text REFERENCES geo.tracts (geoid),
    inserted_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.openaq_measurements (
    id bigserial PRIMARY KEY,
    source text NOT NULL DEFAULT 'openaq',
    fetched_at timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS normalized.air_quality_measurements (
    measurement_id text PRIMARY KEY,
    provider text,
    location_id text,
    sensor_id text,
    parameter text NOT NULL,
    value double precision NOT NULL,
    unit text NOT NULL,
    measured_at timestamptz NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    geom geometry(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
    ) STORED,
    county_geoid text REFERENCES geo.counties (geoid),
    tract_geoid text REFERENCES geo.tracts (geoid),
    inserted_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.smoke_risk_scores (
    id bigserial PRIMARY KEY,
    geography_type text NOT NULL CHECK (geography_type IN ('county', 'tract')),
    geoid text NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    fire_count integer NOT NULL DEFAULT 0,
    max_frp double precision,
    avg_pm25 double precision,
    avg_pm10 double precision,
    risk_score double precision NOT NULL,
    risk_band text NOT NULL CHECK (risk_band IN ('low', 'moderate', 'high', 'severe')),
    computed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT smoke_risk_scores_window_unique UNIQUE (geography_type, geoid, window_start, window_end)
);
