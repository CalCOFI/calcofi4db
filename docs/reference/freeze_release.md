# Freeze a release of the DuckLake

Creates an immutable frozen release from the current Working DuckLake
state. Strips provenance columns, exports tables to Parquet, creates a
catalog, and uploads to `gs://calcofi-db/ducklake/releases/{version}/`.

## Usage

``` r
freeze_release(
  con,
  version,
  release_notes = NULL,
  validate = TRUE,
  tables = NULL,
  dry_run = FALSE,
  bucket = "calcofi-db"
)
```

## Arguments

- con:

  DuckDB connection from
  [`get_working_ducklake()`](https://calcofi.io/calcofi4db/reference/get_working_ducklake.md)

- version:

  Version string in format "vYYYY.MM.DD" (e.g., "v2026.03.14")

- release_notes:

  Character string with release notes, or path to markdown file

- validate:

  Run validation checks before freezing (default: TRUE)

- tables:

  Character vector of tables to include (default: all non-system tables)

- dry_run:

  If TRUE, simulate without uploading (default: FALSE)

- bucket:

  GCS bucket name (default: "calcofi-db")

## Value

List with:

- `version`: The version string

- `gcs_path`: GCS path to release

- `tables`: Tibble with table names and row counts

- `parquet_files`: Paths to created Parquet files

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_working_ducklake()
result <- freeze_release(
  con           = con,
  version       = "v2026.03.14",
  release_notes = "First release with bottle and larvae data.")
} # }
```
