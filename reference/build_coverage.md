# The coverage cube behind the explorer's first paint

n observations and root samples by dataset, by dataset x station x year,
by dataset x year and by dataset x measurement type (with year and depth
spans, and — when the `measurement_type` table carries them — the
registry's `category` and `variable`); the per-station year x month
detail is
[`build_coverage_stations()`](https://calcofi.io/calcofi4db/reference/build_coverage_stations.md),
a second sidecar fetched on demand — small enough to paint the grid
before DuckDB-WASM wakes up, and the variable-based inventory Task 14
asks for. Since 3.25.0 also `taxa[]` (explorer UI plan D14): one row per
taxon of the bio realm — key, names, rank, class, n_obs, year span, life
stages and its datasets with n_obs each — so the organism list opens
before the engine is warm and *Browse* can list organisms by category or
dataset. Deterministic: no wall clock, so a re-run over unchanged inputs
writes identical bytes.

## Usage

``` r
build_coverage(con, version)
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

A list ready for `jsonlite::write_json(auto_unbox = TRUE)`.
