# Database doctor

Structural checks for Postgres drift (schemas, key tables/views, single **`fn_alert_candidates`** overload with **23** parameters, selectable calibration export views).

```bash
make db-doctor
```

**`DB_DOCTOR_WARN_ONLY=1`** prints failures but exits **0**.

Requires a reachable database (**`POSTGRES_*`** / **`PSYCOPG_CONNINFO`**) and applied migrations. Typical healthy rows include **`fn_alert_candidates:single_overload yes`** and **`param_count`** matching **`expected=23`**.
