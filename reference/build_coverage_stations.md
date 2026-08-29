# The per-station coverage card: n obs by dataset x year and by dataset x month, for one station

What db-viz-station draws when a station is clicked — every dataset
sampled there, its rows per year (`years`: `[[year, n], …]`) and per
month (`months`: twelve counts) — for all 218 stations. Kept out of
`coverage.json` so the first paint stays small; fetched when a station
is selected.

## Usage

``` r
build_coverage_stations(con, version)
```

## Arguments

- con:

  DuckDB connection holding `obs` (with `dataset_key`, `grid_key`,
  `datetime`, `taxon_key`, `life_stage`) and `sample_root`; `taxon` (for
  `taxa[]`) and `measurement_type` (for the two variable fields) when
  present.

- version:

  the release version string.

## Value

A list
`{version, stations: [{grid_key, datasets: [{dataset_key, n_obs, year_min, year_max, years, months}]}]}`.
