# Save Working DuckLake to GCS

Uploads the Working DuckLake database file to Google Cloud Storage. This
should be called after ingestion workflows complete to persist changes.

## Usage

``` r
save_working_ducklake(
  con,
  gcs_path = "gs://calcofi-db/ducklake/working/calcofi.duckdb",
  checkpoint = TRUE
)
```

## Arguments

- con:

  DuckDB connection from
  [`get_working_ducklake()`](https://calcofi.io/calcofi4db/reference/get_working_ducklake.md)

- gcs_path:

  GCS destination path (default:
  "gs://calcofi-db/ducklake/working/calcofi.duckdb")

- checkpoint:

  Run checkpoint before upload (default: TRUE)

## Value

GCS URI of uploaded file

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_working_ducklake()
# ... perform ingestion ...
save_working_ducklake(con)
close_duckdb(con)
} # }
```
