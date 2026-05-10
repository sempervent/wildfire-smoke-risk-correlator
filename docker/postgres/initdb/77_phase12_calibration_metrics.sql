-- Phase 12 calibration mirror — aligned with sql/migrations/012_phase12_calibration_metrics.sql.

ALTER TABLE analytics.risk_observations
    ADD COLUMN IF NOT EXISTS units text,
    ADD COLUMN IF NOT EXISTS parameter text,
    ADD COLUMN IF NOT EXISTS lag_hours double precision,
    ADD COLUMN IF NOT EXISTS confidence_label text,
    ADD COLUMN IF NOT EXISTS quality_flags jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE analytics.risk_model_evaluations
    ADD COLUMN IF NOT EXISTS precision_like_high_risk double precision,
    ADD COLUMN IF NOT EXISTS recall_like_high_risk double precision,
    ADD COLUMN IF NOT EXISTS false_positive_like_count integer,
    ADD COLUMN IF NOT EXISTS false_negative_like_count integer,
    ADD COLUMN IF NOT EXISTS insufficient_data_reason text,
    ADD COLUMN IF NOT EXISTS confidence_label text;

ALTER TABLE analytics.dispersion_aq_comparisons
    ADD COLUMN IF NOT EXISTS avg_dispersion_score double precision,
    ADD COLUMN IF NOT EXISTS max_risk_score_v5 double precision,
    ADD COLUMN IF NOT EXISTS avg_risk_score_v5 double precision,
    ADD COLUMN IF NOT EXISTS fire_detection_count integer,
    ADD COLUMN IF NOT EXISTS dispersion_exposure_count integer,
    ADD COLUMN IF NOT EXISTS lag_window text,
    ADD COLUMN IF NOT EXISTS evidence_label text;

CREATE TABLE IF NOT EXISTS analytics.risk_observation_features (
    risk_observation_feature_id uuid PRIMARY KEY DEFAULT gen_random_uuid (),
    risk_observation_id uuid NOT NULL REFERENCES analytics.risk_observations (risk_observation_id) ON DELETE CASCADE,
    parameter text NOT NULL,
    value double precision,
    unit text,
    observed_at timestamptz NOT NULL,
    lag_hours double precision,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now ()
);

CREATE INDEX IF NOT EXISTS risk_observation_features_obs_idx
    ON analytics.risk_observation_features (risk_observation_id);
