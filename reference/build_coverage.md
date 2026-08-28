# The coverage cube behind the explorer's first paint

n observations and root samples by dataset, by dataset x station x year,
by dataset x year and by dataset x measurement type (with year and depth
spans); the per-station year x month detail is
[`build_coverage_stations()`](https://calcofi.io/calcofi4db/reference/build_coverage_stations.md),
a second sidecar fetched on demand — small enough to paint the grid
before DuckDB-WASM wakes up, and the variable-based inventory Task 14
asks for. Deterministic: no wall clock, so a re-run over unchanged
inputs writes identical bytes.

## Usage

``` r
build_coverage(con, version)
```

## Arguments

- con:

  DuckDB connection holding `obs` (with `dataset_key`, `grid_key`,
  `datetime`) and `sample_root`.

- version:

  the release version string.

## Value

A list ready for `jsonlite::write_json(auto_unbox = TRUE)`.
