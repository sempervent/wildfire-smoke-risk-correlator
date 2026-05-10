-- Phase 14 stub (initdb): safe overload teardown before first bootstrap views pass.
-- Canonical CREATE FUNCTION + v_alert_candidates live in sql/migrations/013_phase14_canonical_alert_function.sql
-- and are applied last by scripts/bootstrap_db.sh (after dependent views).
--
-- Fresh volumes have no analytics.fn_alert_candidates yet; this DO block is a no-op.

DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT p.oid,
           pg_get_function_identity_arguments(p.oid) AS args
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'analytics'
      AND p.proname = 'fn_alert_candidates'
      AND p.prokind = 'f'
  LOOP
    EXECUTE format('DROP FUNCTION IF EXISTS analytics.fn_alert_candidates(%s) CASCADE', r.args);
  END LOOP;
END $$;
