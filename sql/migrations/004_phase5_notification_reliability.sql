-- Phase 5: notification attempt audit trail + operational run bookkeeping + operator evidence views.

CREATE TABLE IF NOT EXISTS analytics.notification_attempts (
    notification_attempt_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_event_id uuid NOT NULL REFERENCES analytics.alert_events (alert_event_id) ON DELETE CASCADE,
    notifier text NOT NULL,
    destination_hash text,
    status text NOT NULL CHECK (status IN ('attempted', 'succeeded', 'failed', 'skipped')),
    attempted_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    error_class text,
    error_message text,
    response_code integer,
    retry_after timestamptz,
    payload_hash text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS notification_attempts_alert_event_idx
    ON analytics.notification_attempts (alert_event_id);

CREATE INDEX IF NOT EXISTS notification_attempts_notifier_idx
    ON analytics.notification_attempts (notifier);

CREATE INDEX IF NOT EXISTS notification_attempts_status_idx
    ON analytics.notification_attempts (status);

CREATE INDEX IF NOT EXISTS notification_attempts_attempted_at_idx
    ON analytics.notification_attempts (attempted_at DESC);

CREATE INDEX IF NOT EXISTS notification_attempts_retry_after_idx
    ON analytics.notification_attempts (retry_after);

COMMENT ON TABLE analytics.notification_attempts IS
    'Append-only notifier delivery audit; never store raw URLs/secrets—use destination_hash + safe error text only.';

CREATE TABLE IF NOT EXISTS analytics.operational_runs (
    operational_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    mode text NOT NULL CHECK (mode IN ('fixture', 'live')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    steps jsonb NOT NULL DEFAULT '[]'::jsonb,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS operational_runs_started_at_idx
    ON analytics.operational_runs (started_at DESC);

COMMENT ON TABLE analytics.operational_runs IS
    'High-level operational cycle executions (fixture/live) for Grafana/SQL inspection; steps JSON is non-secret status only.';

CREATE OR REPLACE VIEW analytics.v_open_alert_events AS
SELECT
    alert_event_id,
    fingerprint,
    alert_type,
    severity,
    geography_type,
    geoid,
    title,
    description,
    observed_at,
    first_seen_at,
    last_seen_at,
    status,
    runbook_slug,
    notification_state,
    created_at,
    updated_at
FROM analytics.alert_events
WHERE status = 'open';

COMMENT ON VIEW analytics.v_open_alert_events IS
    'Operator-facing projection of open incidents (presentation only; canonical evaluation remains fn_alert_candidates).';

CREATE OR REPLACE VIEW analytics.v_notification_attempt_summary AS
SELECT
    notifier,
    status,
    COUNT(*)::bigint AS attempt_count,
    MAX(attempted_at) AS last_attempted_at
FROM analytics.notification_attempts
GROUP BY notifier, status;

CREATE OR REPLACE VIEW analytics.v_notification_failures AS
SELECT
    notification_attempt_id,
    alert_event_id,
    notifier,
    destination_hash,
    attempted_at,
    completed_at,
    error_class,
    error_message,
    response_code,
    retry_after,
    payload_hash
FROM analytics.notification_attempts
WHERE status = 'failed';

CREATE OR REPLACE VIEW analytics.v_alert_delivery_state AS
SELECT
    ae.alert_event_id,
    ae.fingerprint,
    ae.alert_type,
    ae.severity,
    ae.status AS alert_status,
    ae.last_seen_at,
    na.notifier,
    na.last_status,
    na.last_attempted_at,
    na.success_count,
    na.failure_count,
    na.skipped_count
FROM analytics.alert_events ae
LEFT JOIN LATERAL (
    SELECT
        notifier,
        (
            SELECT na2.status
            FROM analytics.notification_attempts na2
            WHERE na2.alert_event_id = ae.alert_event_id
              AND na2.notifier = na.notifier
            ORDER BY na2.attempted_at DESC
            LIMIT 1
        ) AS last_status,
        MAX(na.attempted_at) AS last_attempted_at,
        COUNT(*) FILTER (WHERE na.status = 'succeeded')::bigint AS success_count,
        COUNT(*) FILTER (WHERE na.status = 'failed')::bigint AS failure_count,
        COUNT(*) FILTER (WHERE na.status = 'skipped')::bigint AS skipped_count
    FROM analytics.notification_attempts na
    WHERE na.alert_event_id = ae.alert_event_id
    GROUP BY na.notifier
) na ON true
WHERE ae.status = 'open';

CREATE OR REPLACE VIEW analytics.v_recent_operational_cycles AS
SELECT
    operational_run_id,
    mode,
    started_at,
    finished_at,
    status,
    steps,
    error_message,
    created_at
FROM analytics.operational_runs;
