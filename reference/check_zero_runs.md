# Runs of exact zeros within a cast

Finds each run of **exact zeros** in a series within one cast:
consecutive values, in depth order, that are all `0`, hold at least
`min_n` finite values and span at least `min_span_m` metres. A non-zero
value ends a run; so does a step in depth larger than `max_gap_m`
between two zeros (a missing bin or two does not). It tests a different
thing from
[`check_depth_constant_series()`](https://calcofi.io/calcofi4db/reference/check_depth_constant_series.md),
which asks whether the **whole** cast holds one value: a cast of zeros
only is found by both, so a caller that withholds both applies that one
first and this one to what remains. Only an exact `0` counts: a series
that approaches zero (0.003, 0.001) is a measurement.

## Usage

``` r
check_zero_runs(
  x,
  tbl = "ctd_measurement",
  cast_col = "ctd_cast_uuid",
  type_col = "measurement_type",
  value_col = "measurement_value",
  depth_col = "depth_m",
  types = NULL,
  min_n = 6,
  min_span_m = 50,
  max_gap_m = 5
)
```

## Arguments

- x:

  a `DBIConnection` (then `tbl` names the long table) or a data frame
  with the long-format columns below.

- tbl:

  table name on `con`; ignored when `x` is a data frame.

- cast_col, type_col, value_col, depth_col:

  column names: the cast key, the measurement type, the value and the
  depth in metres. The defaults are the CTD ingest's `ctd_measurement`.
  `cast_col` may name **several** columns, which together identify a
  cast (e.g. `c("cruise_key", "cast_key", "cast_dir")`). Group by the
  real cast: a key that is unique per depth scan (the CTD ingest's
  `ctd_cast_uuid` hashes the scan's time) gives one value per group, so
  nothing is ever judged.

- types:

  optional character vector of `measurement_type`s to judge; the default
  `NULL` judges every type present.

- min_n:

  minimum values in a run (default 6).

- min_span_m:

  minimum depth span of a run, metres (default 50).

- max_gap_m:

  largest depth step, metres, between two zeros of one run (default 5:
  CTD bins are 1 m, and a step of 2-5 m is a scan or two removed).

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html), one row
per run, ordered by type, cast and depth: the `cast_col` column(s),
`measurement_type`, `run` (1, 2, ... within the cast and type), `n`,
`depth_min_m`, `depth_max_m`, `span_m`. Zero rows when none. Every value
of a cast and type at a depth in `[depth_min_m, depth_max_m]` that is
exactly 0 belongs to the run (a non-zero value there would have ended
it).

## Details

Which series to test is the caller's decision, and it matters. An exact
0 is a fill or a failed regression where the quantity cannot be zero
(oxygen at 500 m off California beside a sensor reading 15-30 umol/kg),
but it can be a real estimate clipped at zero where the quantity truly
vanishes (nitrate in a depleted surface layer, chlorophyll below the
chlorophyll layer); pass `types` accordingly.

## See also

[`check_depth_constant_series()`](https://calcofi.io/calcofi4db/reference/check_depth_constant_series.md)
for a series constant over the whole cast.

## Examples

``` r
d <- data.frame(cast = "a", measurement_type = "oxygen_sta_corr",
  depth_m = 0:120, measurement_value = c(seq(250, 30, length.out = 60), rep(0, 61)))
check_zero_runs(d, cast_col = "cast")
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> ℹ /tmp/Rtmp9Va2l7/duckdb
#> This is removed when the R session ends.
#> • Extensions are re-downloaded each session.
#> • Secrets are lost.
#> ℹ Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> ℹ Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> ℹ See ?duckdb_storage for details and alternatives.
#> # A tibble: 1 × 7
#>   cast  measurement_type   run     n depth_min_m depth_max_m span_m
#>   <chr> <chr>            <int> <dbl>       <dbl>       <dbl>  <dbl>
#> 1 a     oxygen_sta_corr      1    61          60         120     60
```
