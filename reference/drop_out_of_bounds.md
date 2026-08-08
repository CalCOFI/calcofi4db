# Delete values outside their declared bounds

Enforcement, kept separate from
[`check_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/check_measurement_bounds.md)
so that a bound must be *agreed* before it is allowed to delete data.
Run the check first, put anything surprising to the provider as a
question, and call this only for bounds you are confident describe the
impossible.

## Usage

``` r
drop_out_of_bounds(
  con,
  tbl = "obs",
  mt = NULL,
  dataset_key = NULL,
  type_col = "measurement_type",
  value_col = "measurement_value",
  quiet = FALSE
)
```

## Arguments

- con:

  DuckDB connection

- tbl:

  table or view to check (default `"obs"`). Works on any long-format
  table: the per-dataset `{dataset}_measurement` during wrangling, the
  emitted `obs`, or `sample_measurement`.

- mt:

  the measurement registry: a data.frame, a path to
  `measurement_type.csv`, or `NULL` (default) to read a
  `measurement_type` table from `con`.

- dataset_key:

  optional `dataset_key` to filter to, when `tbl` holds more than one
  dataset. Ignored if `tbl` has no `dataset_key` column.

- type_col, value_col:

  column names (default `measurement_type` / `measurement_value`)

- quiet:

  suppress the summary message

## Value

The pre-delete tally from
[`check_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/check_measurement_bounds.md),
invisibly, restricted to the `out_of_range` rows that were acted on.
`n_bad` is what was deleted per type.

## Details

DELETE rather than flag, for the same reason the `-99` sentinel is
deleted: in a long-format table a row IS an assertion that a value was
measured. A pH of -10 left in place silently corrupts every mean,
minimum and anomaly a consumer computes downstream, and there is no
in-band way to mark it as not-a-value.

Bounds are meant to be **generous** — impossible, not merely unusual —
so this drops nothing an oceanographer would want to see. If it removes
something interesting, the bound is wrong, not the reading.

## Examples

``` r
if (FALSE) { # \dontrun{
oob <- drop_out_of_bounds(con, "ctd_measurement",
                          mt = here::here("metadata/measurement_type.csv"))
} # }
```
