-- Phase 12: calibration / evaluation presentation views (not validation).

CREATE OR REPLACE VIEW analytics.v_dispersion_aq_evidence_summary AS
SELECT
  model_version,
  evidence_label,
  COUNT(*)::bigint AS comparison_row_count,
  MAX(computed_at) AS newest_computed_at
FROM analytics.dispersion_aq_comparisons
GROUP BY model_version, evidence_label;

COMMENT ON VIEW analytics.v_dispersion_aq_evidence_summary IS
  'Counts of dispersion–AQ comparison rows by qualitative evidence label.';

CREATE OR REPLACE VIEW analytics.v_dispersion_aq_lag_summary AS
SELECT
  model_version,
  lag_bucket,
  COUNT(*)::bigint AS row_count,
  AVG(aq_observation_count)::double precision AS avg_aq_observation_count,
  AVG(max_dispersion_score)::double precision AS avg_max_dispersion_score,
  SUM(
    CASE WHEN evidence_label IN ('no_aq_data', 'insufficient_aq_data') THEN 1 ELSE 0 END
  )::bigint AS thin_evidence_rows
FROM analytics.dispersion_aq_comparisons
GROUP BY model_version, lag_bucket;

COMMENT ON VIEW analytics.v_dispersion_aq_lag_summary IS
  'Lag-bucket rollup for dispersion vs AQ comparisons.';

CREATE OR REPLACE VIEW analytics.v_risk_model_evaluation_latest AS
SELECT DISTINCT ON (model_version)
  risk_model_evaluation_id,
  model_version,
  evaluated_at,
  window_start,
  window_end,
  observation_type,
  match_count,
  mae,
  rmse,
  correlation,
  precision_like_high_risk,
  recall_like_high_risk,
  false_positive_like_count,
  false_negative_like_count,
  insufficient_data_reason,
  confidence_label,
  summary,
  created_at
FROM analytics.risk_model_evaluations
ORDER BY model_version, evaluated_at DESC NULLS LAST;

COMMENT ON VIEW analytics.v_risk_model_evaluation_latest IS
  'Most recent evaluation batch per model_version.';

CREATE OR REPLACE VIEW analytics.v_risk_model_evaluation_history AS
SELECT
  risk_model_evaluation_id,
  model_version,
  evaluated_at,
  window_start,
  window_end,
  observation_type,
  match_count,
  mae,
  rmse,
  correlation,
  precision_like_high_risk,
  recall_like_high_risk,
  false_positive_like_count,
  false_negative_like_count,
  insufficient_data_reason,
  confidence_label,
  summary,
  created_at
FROM analytics.risk_model_evaluations
ORDER BY evaluated_at DESC NULLS LAST;

COMMENT ON VIEW analytics.v_risk_model_evaluation_history IS
  'Evaluation batches newest-first.';

CREATE OR REPLACE VIEW analytics.v_model_overprediction_candidates AS
SELECT *
FROM analytics.dispersion_aq_comparisons
WHERE evidence_label = 'possible_overprediction'
  AND computed_at >= (now() - INTERVAL '72 hours');

COMMENT ON VIEW analytics.v_model_overprediction_candidates IS
  'Rows flagged by heuristic high-dispersion / low-PM2.5 mismatch (not causal).';

CREATE OR REPLACE VIEW analytics.v_model_underprediction_candidates AS
SELECT *
FROM analytics.dispersion_aq_comparisons
WHERE evidence_label = 'possible_underprediction'
  AND computed_at >= (now() - INTERVAL '72 hours');

COMMENT ON VIEW analytics.v_model_underprediction_candidates IS
  'Rows flagged by heuristic low-dispersion / high-PM2.5 mismatch (not causal).';

CREATE OR REPLACE VIEW analytics.v_calibration_confidence_summary AS
SELECT
  confidence_label,
  COUNT(*)::bigint AS evaluation_batches,
  MAX(evaluated_at) AS newest_evaluated_at
FROM analytics.risk_model_evaluations
GROUP BY confidence_label;

COMMENT ON VIEW analytics.v_calibration_confidence_summary IS
  'Counts of evaluation batches by qualitative confidence label.';

CREATE OR REPLACE VIEW analytics.v_risk_observation_coverage AS
SELECT
  geography_type,
  geoid,
  COUNT(*)::bigint AS observation_rows,
  MAX(observed_at) AS newest_observation_at
FROM analytics.risk_observations
GROUP BY geography_type, geoid;

COMMENT ON VIEW analytics.v_risk_observation_coverage IS
  'Observation density by geography for calibration coverage spot-checks.';
