# List available frozen releases

Lists all available frozen DuckLake releases with metadata.

## Usage

``` r
list_frozen_releases(bucket = "calcofi-db")
```

## Arguments

- bucket:

  GCS bucket name (default: "calcofi-db")

## Value

Tibble with columns:

- `version`: Version string (e.g., "v2026.02")

- `release_date`: When release was created (POSIXct)

- `tables`: Number of tables

- `total_rows`: Total row count across all tables

- `size_mb`: Total size in megabytes

- `is_latest`: TRUE if this is the latest release

## Examples

``` r
if (FALSE) { # \dontrun{
releases <- list_frozen_releases()
releases
} # }
```
