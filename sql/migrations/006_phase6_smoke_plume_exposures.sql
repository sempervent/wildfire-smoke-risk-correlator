-- Phase 6 (plume): wind corridor approximation exposures (NOT atmospheric dispersion or health guidance).

CREATE TABLE IF NOT EXISTS analytics.smoke_plume_exposures (
    plume_exposure_id bigserial PRIMARY KEY,
    model_version text NOT NULL DEFAULT 'wind_v1',
    detection_id text NOT NULL REFERENCES normalized.fire_detections (detection_id),
    geography_type text NOT NULL CHECK (geography_type IN ('county', 'tract')),
    geoid text NOT NULL,
    wind_observation_id text REFERENCES normalized.wind_observations (wind_observation_id),
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    distance_km double precision,
    bearing_from_fire_degrees double precision,
    wind_from_degrees double precision,
    downwind_bearing_degrees double precision,
    angular_error_degrees double precision,
    wind_speed_mps double precision,
    exposure_score double precision NOT NULL,
    explanation jsonb NOT NULL DEFAULT '{}'::jsonb,
    computed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT smoke_plume_exposures_dedupe UNIQUE (
        detection_id,
        geography_type,
        geoid,
        window_start,
        window_end,
        model_version
    )
);

CREATE INDEX IF NOT EXISTS smoke_plume_exposures_detection_idx ON analytics.smoke_plume_exposures (detection_id);
CREATE INDEX IF NOT EXISTS smoke_plume_exposures_geo_idx ON analytics.smoke_plume_exposures (geography_type, geoid);
CREATE INDEX IF NOT EXISTS smoke_plume_exposures_computed_at_idx ON analytics.smoke_plume_exposures (computed_at DESC);
CREATE INDEX IF NOT EXISTS smoke_plume_exposures_score_idx ON analytics.smoke_plume_exposures (exposure_score DESC);

COMMENT ON TABLE analytics.smoke_plume_exposures IS 'Engineering corridor approximation linking fires + wind + downwind geographies; not a dispersion model.';
