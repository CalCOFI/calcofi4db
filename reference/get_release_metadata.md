# Get metadata for a frozen release

Retrieves detailed metadata for a specific frozen DuckLake release
including version info, table schemas, and statistics.

## Usage

``` r
get_release_metadata(version = "latest", bucket = "calcofi-db")
```

## Arguments

- version:

  Version string (e.g., "v2026.02") or "latest" (default)

- bucket:

  GCS bucket name (default: "calcofi-db")

## Value

List with:

- `version`: Version string

- `release_date`: Release timestamp

- `release_notes`: Release notes (character)

- `tables`: Tibble with table metadata (name, rows, columns, size_bytes)

- `gcs_path`: GCS path to release files

## Examples

``` r
if (FALSE) { # \dontrun{
meta <- get_release_metadata()
meta <- get_release_metadata("v2026.02")
meta$tables
} # }
```
