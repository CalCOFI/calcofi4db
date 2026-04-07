# Compare two frozen releases

Generates a diff report between two frozen releases showing added,
removed, and modified tables and schema changes.

## Usage

``` r
compare_releases(v1, v2, bucket = "calcofi-db")
```

## Arguments

- v1:

  First version string (e.g., "v2026.02")

- v2:

  Second version string (e.g., "v2026.03")

- bucket:

  GCS bucket name (default: "calcofi-db")

## Value

List with:

- `summary`: Tibble with table-level summary (table, rows_v1, rows_v2,
  change)

- `tables_added`: Tables in v2 but not v1

- `tables_removed`: Tables in v1 but not v2

- `row_changes`: Tibble with row count changes

## Examples

``` r
if (FALSE) { # \dontrun{
diff <- compare_releases("v2026.02", "v2026.03")
diff$summary
diff$tables_added
} # }
```
