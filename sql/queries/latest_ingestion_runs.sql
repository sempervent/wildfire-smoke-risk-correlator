-- Latest ingestion runs by source (adjust LIMIT as needed).
SELECT
  run_id,
  source,
  mode,
  started_at,
  finished_at,
  status,
  records_fetched,
  records_published,
  records_failed,
  config,
  error_message
FROM analytics.ingestion_runs
ORDER BY started_at DESC
LIMIT 50;
