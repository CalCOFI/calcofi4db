# Validate Working DuckLake for release

Runs data quality checks on the Working DuckLake before creating a
frozen release. Checks include: null required fields, valid value
ranges, foreign key integrity, row count expectations, and data
completeness.

## Usage

``` r
validate_for_release(con, checks = "all", strict = FALSE, config = NULL)
```

## Arguments

- con:

  DuckDB connection from
  [`get_working_ducklake()`](https://calcofi.io/calcofi4db/reference/get_working_ducklake.md)

- checks:

  Character vector of check names to run, or "all" (default). Available
  checks: "nulls", "ranges", "foreign_keys", "row_counts",
  "completeness"

- strict:

  Logical, fail on warnings as well as errors (default: FALSE)

- config:

  Optional list of validation configuration (expected row counts, etc.)

## Value

List with:

- `passed`: Logical, TRUE if all checks passed

- `checks`: Tibble with individual check results (name, status, message)

- `errors`: Character vector of error messages

- `warnings`: Character vector of warning messages

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_working_ducklake()
validation <- validate_for_release(con)
if (validation$passed) {
  freeze_release(con, version = "v2026.02")
}
} # }
```
