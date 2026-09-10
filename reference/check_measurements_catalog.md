# Check a `measurements.json` against the release it was built from

One row per check with an `ok` flag, so a release chunk can print the
table whether or not it passes. The counts are re-measured against
`obs_env` when `con` is given (the release's own connection, or a
promoted release read back); without it the arithmetic that lives inside
the record is still checked.

## Usage

``` r
check_measurements_catalog(
  record,
  con = NULL,
  dataset_record = NULL,
  measurement_type = NULL
)
```

## Arguments

- record:

  from
  [`build_measurements_catalog()`](https://calcofi.io/calcofi4db/reference/build_measurements_catalog.md)
  (or a path or URL)

- con:

  a DBI connection holding `obs_env`; NULL to check only what the record
  can prove about itself

- dataset_record:

  the `datasets.json` record (or its path) whose `datasets[]` every
  `dataset_key` must appear in; NULL to skip that check

- measurement_type:

  the registry every `series[].measurement_type` must be in (a data
  frame, or NULL to skip)

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html): `check`,
`level`, `ok`, `expected`, `observed`, `detail`.

## See also

[`assert_measurements_catalog()`](https://calcofi.io/calcofi4db/reference/assert_measurements_catalog.md),
[`measurements_catalog_checks()`](https://calcofi.io/calcofi4db/reference/measurements_catalog_checks.md)
