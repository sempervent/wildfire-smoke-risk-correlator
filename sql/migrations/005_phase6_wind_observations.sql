-- Phase 6 (wind): raw + normalized wind observation storage (engineering telemetry; not dispersion modeling).

CREATE TABLE IF NOT EXISTS raw.wind_observations (
    id bigserial PRIMARY KEY,
    source text NOT NULL,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS normalized.wind_observations (
    wind_observation_id text PRIMARY KEY,
    source text NOT NULL,
    station_id text,
    observed_at timestamptz NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    wind_speed_mps double precision,
    wind_direction_degrees double precision,
    wind_gust_mps double precision,
    geom geometry(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
    ) STORED,
    county_geoid text REFERENCES geo.counties (geoid),
    tract_geoid text REFERENCES geo.tracts (geoid),
    inserted_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS wind_observations_geom_gix ON normalized.wind_observations USING gist (geom);
CREATE INDEX IF NOT EXISTS wind_observations_observed_at_idx ON normalized.wind_observations (observed_at DESC);
CREATE INDEX IF NOT EXISTS wind_observations_station_id_idx ON normalized.wind_observations (station_id);
CREATE INDEX IF NOT EXISTS wind_observations_county_geoid_idx ON normalized.wind_observations (county_geoid);
CREATE INDEX IF NOT EXISTS wind_observations_tract_geoid_idx ON normalized.wind_observations (tract_geoid);

COMMENT ON TABLE raw.wind_observations IS 'Kafka-backed wind payloads prior to normalization.';
COMMENT ON TABLE normalized.wind_observations IS 'Point wind observations/forecasts (meteorological wind FROM direction).';
