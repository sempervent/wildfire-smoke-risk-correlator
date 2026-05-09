-- Phase 4: persisted alert events with deduplication for notification routing.

CREATE TABLE IF NOT EXISTS analytics.alert_events (
    alert_event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    fingerprint text NOT NULL,
    alert_type text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('info', 'warning', 'high', 'critical')),
    geography_type text,
    geoid text,
    title text NOT NULL,
    description text NOT NULL,
    observed_at timestamptz NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acknowledged', 'resolved')),
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    notification_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    runbook_slug text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alert_events_status_last_seen_idx
    ON analytics.alert_events (status, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS alert_events_alert_type_idx
    ON analytics.alert_events (alert_type);

COMMENT ON TABLE analytics.alert_events IS
    'Materialized alert incidents derived from analytics.fn_alert_candidates; duplicates collapse via fingerprint while status is open or acknowledged. Canonical freshness logic remains in SQL views/functions.';

-- One active incident row per fingerprint while triage is incomplete.
CREATE UNIQUE INDEX IF NOT EXISTS alert_events_open_fingerprint_uidx
    ON analytics.alert_events (fingerprint)
    WHERE status IN ('open', 'acknowledged');

CREATE OR REPLACE FUNCTION analytics.touch_alert_events_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_alert_events_touch_updated_at ON analytics.alert_events;
CREATE TRIGGER trg_alert_events_touch_updated_at
    BEFORE UPDATE ON analytics.alert_events
    FOR EACH ROW
    EXECUTE FUNCTION analytics.touch_alert_events_updated_at();
