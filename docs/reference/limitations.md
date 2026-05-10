# Limitations

- **Not** a public-health advisory, **not** emergency guidance, **not** regulatory air-quality compliance modeling.
- **Wind discovery** lists stations in bbox — not a full mesonet; narrow **`WIND_BBOX`** / use **`WIND_STATION_IDS`**.
- **Fixture timestamps** are often stale vs freshness SLIs — use **`ALERTS_WARN_ONLY=1`** for demos.
- **National Census loads** are intentionally heavy — off by default.
- **Default FIRMS/OpenAQ bbox** may span CONUS while Census defaults may be a single state — points outside loaded geometries miss **`geoid`** linkage.
- **Spark jobs are batch** (offset windows), not continuously committed streaming.
- **Grid weather / v4 / v5** need populated prerequisite tables — empty windows can yield zero derived rows legitimately.
- **Notifier paths** must never log SMTP passwords or raw webhook URLs with tokens.
