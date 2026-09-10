# Validate a `measurements.json` against the package's JSON schema

The schema is `inst/schema/measurements.schema.json` (draft-07). Uses
jsonvalidate when installed; otherwise a structural check of the
required top-level and per-measurement keys, which is what the tests can
always run.

## Usage

``` r
validate_measurements_catalog(
  x,
  schema = system.file("schema", "measurements.schema.json", package = "calcofi4db"),
  verbose = TRUE
)
```

## Arguments

- x:

  a `measurements.json` path, its text, or the record list

- schema:

  path to the schema file

- verbose:

  return the validator's error table on failure

## Value

`TRUE`, or stops with the first errors.

## See also

[`build_measurements_catalog()`](https://calcofi.io/calcofi4db/reference/build_measurements_catalog.md),
[`check_measurements_catalog()`](https://calcofi.io/calcofi4db/reference/check_measurements_catalog.md)
