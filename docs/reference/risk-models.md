# Risk models (engineering index)

Bands are consistent across models:

- **low**: \[0, 25)
- **moderate**: \[25, 50)
- **high**: \[50, 75)
- **severe**: \[75, 100\]

## v1

Legacy composite from fire counts/FRP and geography-linked AQ averages.

## v2

Spatial signal: fires inside polygon vs nearby radius (**`SMOKE_RISK_NEARBY_KM`**), max FRP, AQ averages, recency. Stores **`explanation` JSONB**.

## v3

Blends **v2 base** with **`wind_v1`** plume exposure component.

## v4

Uses **`wind_grid_v2`** plume when grid weather is available; humidity dampening from matched grid cells. Requires grid pipeline populated.

## v5

Blends base signals with plume + optional **`gaussian_v0`** dispersion proxy + capped humidity dampening — configuration-dependent.

## Plume models

- **`wind_v1`**: station-wind corridor heuristic.
- **`wind_grid_v2`**: prefers matched grid wind.

Not atmospheric CFD or regulatory dispersion.

## `gaussian_v0` (dispersion proxy)

A **bounded Gaussian-style kernel** along/adjacent to the downwind axis over census targets — **not** HYSPLIT, not EPA-grade dispersion, **not** a health model. Compare only as an engineering heuristic alongside plume corridors.

Configure via **`DISPERSION_*`** variables. See **`.env.example`**.
