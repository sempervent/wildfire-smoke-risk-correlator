-- Phase 10 mirror — aligned with sql/migrations/010_phase10_calibration.sql.

CREATE TABLE IF NOT EXISTS analytics.risk_observations (
    risk_observation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    observed_at timestamptz NOT NULL,
    geography_type text NOT NULL CHECK (geography_type IN ('county', 'tract')),
    geoid text NOT NULL,
    observation_type text NOT NULL,
    observed_value double precision,
    observed_band text,
    source text NOT NULL,
    notes text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS risk_observations_geo_idx ON analytics.risk_observations (geography_type, geoid, observed_at DESC);
CREATE INDEX IF NOT EXISTS risk_observations_type_idx ON analytics.risk_observations (observation_type, observed_at DESC);

CREATE TABLE IF NOT EXISTS analytics.risk_model_evaluations (
    risk_model_evaluation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    model_version text NOT NULL,
    evaluated_at timestamptz NOT NULL DEFAULT now(),
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    observation_type text NOT NULL,
    match_count integer NOT NULL DEFAULT 0,
    mae double precision,
    rmse double precision,
    correlation double precision,
    summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS risk_model_evaluations_model_idx ON analytics.risk_model_evaluations (model_version, evaluated_at DESC);
