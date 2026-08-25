# Check depth coordinates against an absolute range

One row per (table, dataset, depth column): how many depths are NaN,
below `min_depth_m` or above `max_depth_m`. A non-`ok` row is an error
in the data — assert `all(status == "ok")` at ingest and at release.

## Usage

``` r
check_depth_bounds(
  con,
  tbls = c("sample", "obs"),
  depth_cols = c("depth_min_m", "depth_max_m"),
  max_depth_m = CC_DEPTH_MAX_M,
  min_depth_m = 0,
  by = "dataset_key"
)
```

## Arguments

- con:

  DBI connection.

- tbls:

  Tables to check (each needs the depth columns it has of `depth_cols`;
  a table lacking all of them is skipped with a message).

- depth_cols:

  Depth columns to check where present.

- max_depth_m, min_depth_m:

  The plausible range.

- by:

  Grouping column (default `dataset_key`; NULL for none).

## Value

A tibble: `table`, `dataset_key`, `depth_col`, `n_total`, `n_nan`,
`n_below`, `n_above`, `v_min`, `v_max`, `status` (`ok` \|
`out_of_range`).
