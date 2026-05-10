-- Phase 2: ingestion run tracking, smoke_risk explainability columns, unique constraint per model.

CREATE TABLE IF NOT EXISTS analytics.ingestion_runs (
    run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('live', 'dry_run')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    records_fetched integer NOT NULL DEFAULT 0,
    records_published integer NOT NULL DEFAULT 0,
    records_failed integer NOT NULL DEFAULT 0,
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ingestion_runs_source_started_idx ON analytics.ingestion_runs (source, started_at DESC);

ALTER TABLE analytics.smoke_risk_scores
    ADD COLUMN IF NOT EXISTS model_version text NOT NULL DEFAULT 'v1';

ALTER TABLE analytics.smoke_risk_scores
    ADD COLUMN IF NOT EXISTS explanation jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE analytics.smoke_risk_scores
    ADD COLUMN IF NOT EXISTS nearby_fire_count integer NOT NULL DEFAULT 0;

ALTER TABLE analytics.smoke_risk_scores
    ADD COLUMN IF NOT EXISTS nearest_fire_km double precision;

ALTER TABLE analytics.smoke_risk_scores
    ADD COLUMN IF NOT EXISTS aq_observation_count integer NOT NULL DEFAULT 0;

ALTER TABLE analytics.smoke_risk_scores
    ADD COLUMN IF NOT EXISTS newest_aq_observed_at timestamptz;

ALTER TABLE analytics.smoke_risk_scores
    ADD COLUMN IF NOT EXISTS newest_fire_observed_at timestamptz;

-- Replace uniqueness to allow v1 and v2 rows for the same window/geography.
ALTER TABLE analytics.smoke_risk_scores DROP CONSTRAINT IF EXISTS smoke_risk_scores_window_unique;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'smoke_risk_scores_window_model_unique'
    ) THEN
        ALTER TABLE analytics.smoke_risk_scores
            ADD CONSTRAINT smoke_risk_scores_window_model_unique
            UNIQUE (geography_type, geoid, window_start, window_end, model_version);
    END IF;
END $$;
