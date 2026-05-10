-- Phase 12: calibration metrics, observation metadata, dispersion–AQ evidence columns (engineering only).

ALTER TABLE analytics.risk_observations
    ADD COLUMN IF NOT EXISTS units text,
    ADD COLUMN IF NOT EXISTS parameter text,
    ADD COLUMN IF NOT EXISTS lag_hours double precision,
    ADD COLUMN IF NOT EXISTS confidence_label text,
    ADD COLUMN IF NOT EXISTS quality_flags jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN analytics.risk_observations.units IS 'Observation units (e.g. µg/m³) — presentation.';
COMMENT ON COLUMN analytics.risk_observations.parameter IS 'Measured parameter (e.g. pm25) when observation_type is coarse.';
COMMENT ON COLUMN analytics.risk_observations.confidence_label IS 'Qualitative data-quality hint — not peer-reviewed.';

ALTER TABLE analytics.risk_model_evaluations
    ADD COLUMN IF NOT EXISTS precision_like_high_risk double precision,
    ADD COLUMN IF NOT EXISTS recall_like_high_risk double precision,
    ADD COLUMN IF NOT EXISTS false_positive_like_count integer,
    ADD COLUMN IF NOT EXISTS false_negative_like_count integer,
    ADD COLUMN IF NOT EXISTS insufficient_data_reason text,
    ADD COLUMN IF NOT EXISTS confidence_label text;

COMMENT ON COLUMN analytics.risk_model_evaluations.insufficient_data_reason IS 'Why metrics were skipped or bounded (e.g. low match_count).';
COMMENT ON COLUMN analytics.risk_model_evaluations.confidence_label IS 'Strength of evidence for this evaluation batch — not validation.';

ALTER TABLE analytics.dispersion_aq_comparisons
    ADD COLUMN IF NOT EXISTS avg_dispersion_score double precision,
    ADD COLUMN IF NOT EXISTS max_risk_score_v5 double precision,
    ADD COLUMN IF NOT EXISTS avg_risk_score_v5 double precision,
    ADD COLUMN IF NOT EXISTS fire_detection_count integer,
    ADD COLUMN IF NOT EXISTS dispersion_exposure_count integer,
    ADD COLUMN IF NOT EXISTS lag_window text,
    ADD COLUMN IF NOT EXISTS evidence_label text;

COMMENT ON COLUMN analytics.dispersion_aq_comparisons.evidence_label IS 'Heuristic alignment label — not causal inference.';
COMMENT ON COLUMN analytics.dispersion_aq_comparisons.lag_window IS 'Redundant human-readable lag span (mirrors lag_bucket).';

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

COMMENT ON TABLE analytics.risk_observation_features IS
  'Optional companion facts per calibration observation — additive to Phase 10 rows.';
