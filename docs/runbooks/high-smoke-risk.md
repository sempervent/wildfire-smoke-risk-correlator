# Elevated smoke risk (`high_smoke_risk`)

## Meaning

Latest county or tract risk snapshot exceeds configured elevated bands/scores (`ALERT_HIGH_RISK_MIN_SCORE`, risk bands `high`/`severe`).

## Likely causes

- Real correlated smoke signals (fire proximity + AQ stress).
- Model tuning / window effects.
- Bad or stale upstream normalization skewing scores.

## First checks

```bash
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT geography_type, geoid, risk_score, risk_band, explanation FROM analytics.v_latest_smoke_risk_by_county ORDER BY risk_score DESC LIMIT 20;"
docker compose exec -T postgres psql -U smoke -d smoke -c \
  "SELECT * FROM analytics.v_sli_high_smoke_risk LIMIT 50;"
```

## Remediation

- Inspect `explanation` JSON for the geography.
- Verify fires/AQ inputs for the scoring window.
- Re-run `make compute-risk` after confirming normalized data.

## Fixture / demo mode

Scores reflect **fixture** landscapes; use these alerts to exercise routing/notifiers, not public safety decisions.
