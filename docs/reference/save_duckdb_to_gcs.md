# Save DuckDB to GCS

Saves a DuckDB database file to Google Cloud Storage.

## Usage

``` r
save_duckdb_to_gcs(con, gcs_path, checkpoint = TRUE)
```

## Arguments

- con:

  DuckDB connection

- gcs_path:

  GCS path (e.g., "gs://calcofi-db/duckdb/calcofi.duckdb")

- checkpoint:

  Run checkpoint before saving to ensure data is flushed

## Value

GCS URI of uploaded file

## Examples

``` r
if (FALSE) { # \dontrun{
save_duckdb_to_gcs(con, "gs://calcofi-db/duckdb/calcofi.duckdb")
} # }
```
