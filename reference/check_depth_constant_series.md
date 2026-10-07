# Casts whose series is constant over depth

Finds each (cast, measurement type) whose values are **identical at
every depth** although the series was measured at many depths over a
real span. A measured or modelled profile of temperature, nitrate,
oxygen or chlorophyll does not do that; a per-cast offset or intercept
written where a profile belongs does. Neither a declared bound
([`check_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/check_measurement_bounds.md))
nor a provider flag can catch it, because the value is physically
possible and unflagged.

## Usage

``` r
check_depth_constant_series(
  x,
  tbl = "ctd_measurement",
  cast_col = "ctd_cast_uuid",
  type_col = "measurement_type",
  value_col = "measurement_value",
  depth_col = "depth_m",
  types = NULL,
  min_n = 6,
  min_span_m = 50,
  tol = 1e-09
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

  minimum finite values on a cast for it to be judged (default 6).

- min_span_m:

  minimum depth span, metres, for it to be judged (default 50).

- tol:

  the series is constant when `max - min` is below this (default 1e-9).

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html), one row
per constant (cast, type), ordered by type then cast: the `cast_col`
column(s), `measurement_type`, `n` (finite values), `depth_min_m`,
`depth_max_m`, `span_m`, `value` (the one value it holds). Zero rows
when nothing is constant. The `judged` attribute is a named integer
vector: per type, how many (cast, type) pairs met `min_n` and
`min_span_m`, so a caller can report "constant on 14 of 14".

## Details

A cast is **judged** only when it has at least `min_n` finite values
spanning at least `min_span_m` metres; a short cast (a bottle cast with
three depths, a surface-only series) is never flagged, because a
constant over two bottles proves nothing. A judged cast is **constant**
when its values' range is below `tol`.

## See also

[`check_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/check_measurement_bounds.md)
for the value's physical range.

## Examples

``` r
d <- data.frame(
  cast = rep(c("a", "b"), each = 8), measurement_type = "est_nitrate",
  depth_m = rep(seq(0, 350, by = 50), 2),
  measurement_value = c(rep(12.5, 8), seq(1, 40, length.out = 8)))
check_depth_constant_series(d, cast_col = "cast")
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> ℹ /tmp/RtmpDrCW28/duckdb
#> This is removed when the R session ends.
#> • Extensions are re-downloaded each session.
#> • Secrets are lost.
#> ℹ Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> ℹ Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> ℹ See ?duckdb_storage for details and alternatives.
#> # A tibble: 1 × 7
#>   cast  measurement_type     n depth_min_m depth_max_m span_m value
#>   <chr> <chr>            <dbl>       <dbl>       <dbl>  <dbl> <dbl>
#> 1 a     est_nitrate          8           0         350    350  12.5
```
