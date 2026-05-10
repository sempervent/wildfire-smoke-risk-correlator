-- Phase 11: bounded Gaussian dispersion proxy + AQ lag comparison scaffolding (engineering only).

CREATE TABLE IF NOT EXISTS analytics.smoke_dispersion_exposures (
    dispersion_exposure_id bigserial PRIMARY KEY,
    model_version text NOT NULL DEFAULT 'gaussian_v0',
    detection_id text NOT NULL REFERENCES normalized.fire_detections (detection_id),
    geography_type text NOT NULL CHECK (geography_type IN ('county', 'tract')),
    geoid text NOT NULL,
    weather_cell_id text REFERENCES normalized.weather_grid_cells (weather_cell_id),
    wind_observation_id text REFERENCES normalized.wind_observations (wind_observation_id),
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    distance_km double precision,
    downwind_distance_km double precision,
    crosswind_distance_km double precision,
    bearing_from_fire_degrees double precision,
    wind_from_degrees double precision,
    downwind_bearing_degrees double precision,
    wind_speed_mps double precision,
    source_strength double precision,
    dispersion_score double precision NOT NULL,
    concentration_proxy double precision,
    explanation jsonb NOT NULL DEFAULT '{}'::jsonb,
    computed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT smoke_dispersion_exposures_dedupe UNIQUE (
        model_version,
        detection_id,
        geography_type,
        geoid,
        window_start,
        window_end
    )
);

CREATE INDEX IF NOT EXISTS smoke_dispersion_model_idx ON analytics.smoke_dispersion_exposures (model_version);
CREATE INDEX IF NOT EXISTS smoke_dispersion_detection_idx ON analytics.smoke_dispersion_exposures (detection_id);
CREATE INDEX IF NOT EXISTS smoke_dispersion_geo_idx ON analytics.smoke_dispersion_exposures (geography_type, geoid);
CREATE INDEX IF NOT EXISTS smoke_dispersion_computed_idx ON analytics.smoke_dispersion_exposures (computed_at DESC);
CREATE INDEX IF NOT EXISTS smoke_dispersion_score_idx ON analytics.smoke_dispersion_exposures (dispersion_score DESC);

COMMENT ON TABLE analytics.smoke_dispersion_exposures IS
  'Engineering Gaussian-ish smoke proxy vs census geographies — not HYSPLIT or regulatory dispersion.';

CREATE TABLE IF NOT EXISTS analytics.dispersion_aq_comparisons (
    dispersion_aq_comparison_id bigserial PRIMARY KEY,
    model_version text NOT NULL,
    geography_type text NOT NULL CHECK (geography_type IN ('county', 'tract')),
    geoid text NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    lag_bucket text NOT NULL,
    lag_hours_lo double precision NOT NULL,
    lag_hours_hi double precision NOT NULL,
    max_dispersion_score double precision,
    avg_pm25 double precision,
    avg_pm10 double precision,
    aq_observation_count integer NOT NULL DEFAULT 0,
    lag_hours double precision,
    comparison_score double precision,
    explanation jsonb NOT NULL DEFAULT '{}'::jsonb,
    computed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT dispersion_aq_comparisons_dedupe UNIQUE (
        model_version,
        geography_type,
        geoid,
        window_start,
        window_end,
        lag_bucket
    )
);

CREATE INDEX IF NOT EXISTS dispersion_aq_comp_geo_idx ON analytics.dispersion_aq_comparisons (geography_type, geoid);
CREATE INDEX IF NOT EXISTS dispersion_aq_comp_window_idx ON analytics.dispersion_aq_comparisons (window_start, window_end);

COMMENT ON TABLE analytics.dispersion_aq_comparisons IS
  'Lag-window AQ summaries vs dispersion exposure — evaluation scaffolding only.';
