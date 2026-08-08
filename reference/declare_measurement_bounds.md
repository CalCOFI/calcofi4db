# Declare `valid_min` / `valid_max` on measurement types that already exist

[`register_measurement_types()`](https://calcofi.io/calcofi4db/reference/register_measurement_types.md)
only ever *appends* — by design, so an ingest cannot silently rewrite a
type another dataset relies on. That leaves no way to do the thing the
bounds convention asks for most often: put a bound on a type that is
already registered without one. All 73 unbounded types at v2026.08.07
were in exactly that state, so "declare it with
[`register_measurement_types()`](https://calcofi.io/calcofi4db/reference/register_measurement_types.md)"
was advice that could not be followed.

## Usage

``` r
declare_measurement_bounds(bounds, path, overwrite = FALSE, quiet = FALSE)
```

## Arguments

- bounds:

  data.frame with `measurement_type` and at least one of `valid_min`,
  `valid_max`, `valid_depth_min_m`, `valid_depth_max_m`. `NA` leaves
  that bound undeclared; supply only the side you can defend.

- path:

  path to `metadata/measurement_type.csv`

- overwrite:

  allow replacing a bound that is already declared (default FALSE). A
  declared bound has been agreed with a provider, so changing it is a
  deliberate act, not a side effect of re-running an ingest.

- quiet:

  suppress the summary message

## Value

The full updated registry, invisibly if nothing changed.

## Details

This is the narrow, auditable counterpart: it changes **only** the four
bound columns, only on rows that already exist, and it refuses an
unknown `measurement_type` rather than quietly adding one — a typo would
otherwise create a bound-carrying orphan that no observation ever
matches.

## See also

[`check_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/check_measurement_bounds.md),
which is what consumes these.

## Examples

``` r
if (FALSE) { # \dontrun{
declare_measurement_bounds(
  data.frame(measurement_type = "zooscan_abundance", valid_min = 0),
  here::here("metadata/measurement_type.csv"))
} # }
```
