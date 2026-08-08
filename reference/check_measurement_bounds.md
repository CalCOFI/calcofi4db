# Check measured values against the registry's declared bounds

The standard bounds check for an ingest notebook and for the release.
Compares every value in a long-format measurement table against
`valid_min` / `valid_max` from `metadata/measurement_type.csv`, and
reports the types that violate a bound **alongside the types that
declare none** — see the note in `R/bounds.R` for why the second matters
at least as much as the first.

## Usage

``` r
check_measurement_bounds(
  con,
  tbl = "obs",
  mt = NULL,
  dataset_key = NULL,
  type_col = "measurement_type",
  value_col = "measurement_value",
  depth_col = NULL,
  include_undeclared = TRUE
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

- depth_col:

  optional depth column enabling the depth-window check against
  `valid_depth_min_m` / `valid_depth_max_m` — the depth over which a
  type is *defined*. A non-null value outside that window is a finding:
  it means the type was emitted where the registry says it does not
  exist.

- include_undeclared:

  report types with no declared bound (default TRUE). Set FALSE for a
  violations-only view.

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html), one row
per measurement type present, ordered worst-first (violations by count,
then undeclared by count):

- `status`:

  `out_of_range` (declared and violated), `undeclared` (nothing
  declared), `ok` (declared and respected)

- `n_total`, `n_bad`, `pct_bad`:

  rows checked, rows outside, percent

- `n_low`, `n_high`:

  split by which bound was broken

- `v_min`, `v_max`:

  observed range, for proposing a bound

- `valid_min`, `valid_max`:

  what the registry declares

- `n_outside_depth`:

  present only when `depth_col` is given

- `finding`:

  a one-line summary, ready to paste into the `context` column of a
  `questions.csv` row

## Details

Read-only: it deletes and rewrites nothing. Enforce with
[`drop_out_of_bounds()`](https://calcofi.io/calcofi4db/reference/drop_out_of_bounds.md),
and only once the bound is agreed.

Bounds may be one-sided. `valid_min = 0` with no `valid_max` is the
useful case for abundances and counts — "never negative" is agreeable
without knowing the ceiling — and a type is `undeclared` only when
*both* are missing.

## See also

[`drop_out_of_bounds()`](https://calcofi.io/calcofi4db/reference/drop_out_of_bounds.md)
to enforce,
[`bounds_datatable()`](https://calcofi.io/calcofi4db/reference/bounds_datatable.md)
to render,
[`read_measurement_type()`](https://calcofi.io/calcofi4db/reference/read_measurement_type.md)
for the registry,
[`register_measurement_types()`](https://calcofi.io/calcofi4db/reference/register_measurement_types.md)
to declare a new bound.

## Examples

``` r
if (FALSE) { # \dontrun{
b <- check_measurement_bounds(
  con, "ctd_measurement",
  mt = here::here("metadata/measurement_type.csv"))
bounds_datatable(b)
} # }
```
