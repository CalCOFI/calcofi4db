# Validate a `datasets.json` against the package's JSON schema

The schema is `inst/schema/datasets.schema.json` (draft-07). Uses
jsonvalidate when installed; otherwise a structural check of the
required top-level and per-record keys, which is what the tests can
always run.

## Usage

``` r
validate_dataset_catalog(
  x,
  schema = system.file("schema", "datasets.schema.json", package = "calcofi4db"),
  verbose = TRUE
)
```

## Arguments

- x:

  a `datasets.json` path, its text, or the record list

- schema:

  path to the schema file

- verbose:

  return the validator's error table on failure

## Value

`TRUE`, or stops with the first errors.
