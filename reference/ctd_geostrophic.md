# Relative geostrophic velocity between adjacent CTD stations

Rasmus Swalethorp's recipe (email 2026-09-22; CalCOFI/workflows#103):
per station, absolute salinity and conservative temperature on a `dp`
dbar grid to `p_ref`, dynamic height anomaly relative to `p_ref`
(`gsw_geo_strf_dyn_height()`), then for each adjacent pair
`v = (dh2 - dh1) / (f * dx)` with `f` the Coriolis parameter at the
pair's mean latitude and `dx` the distance between the two stations.
Positive is 90 degrees to the left of the direction of increasing
station order — for stations ordered nearshore to offshore on a CalCOFI
line (which runs west-south-west) that is equatorward.

## Usage

``` r
ctd_geostrophic(casts, p_ref = 500, dp = 1, min_dx_km = 10)
```

## Arguments

- casts:

  data frame, one row per sample, ordered or orderable by
  `station_order`: columns `station` (id), `latitude`, `longitude`,
  `pressure` (dbar), `temperature` (deg C), `salinity` (PSS-78);
  optional `q_temperature`, `q_salinity`, `station_order` (numeric,
  default the order of first appearance) and `dist_km` (along-line
  distance; default the great-circle distance between the pair).

- p_ref:

  reference pressure (dbar), default 500.

- dp:

  grid spacing (dbar), default 1.

- min_dx_km:

  minimum station spacing (km), default 10. A station closer than this
  to the last station kept is skipped: the geostrophic shear is
  `1 / dx`, so the extra inshore stations a few km apart (the SCCOOS
  90.27.7 beside 90.28, 93.26.4 beside 93.26.7) turn a small density
  difference into metres per second of spurious flow.

## Value

tibble, one row per (station pair x pressure): `station_1`, `station_2`,
`dist_mid_km` (from the first station), `dx_km`, `pressure`,
`velocity_m_s`, `shallow`.

## Details

The flow is relative to `p_ref` (there is no level of known motion), so
it has **no anomaly**. A station shallower than `p_ref` has its deepest
density carried down (`approx(rule = 2)`, as in the recipe) and the pair
is marked `shallow = TRUE`. Flagged temperature or salinity samples are
dropped first; a station with fewer than 2 good samples is skipped.
