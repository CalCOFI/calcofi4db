# Load a GCS Parquet File into DuckDB

Downloads a parquet file from Google Cloud Storage via
[`get_gcs_file()`](https://calcofi.io/calcofi4db/reference/get_gcs_file.md),
creates a DuckDB table from it, and converts any WKB BLOB geometry
columns back to native GEOMETRY type.

## Usage

``` r
load_gcs_parquet_to_duckdb(con, gcs_path, table_name, geom_cols = NULL)
```

## Arguments

- con:

  DBI connection to DuckDB

- gcs_path:

  Character. Full `gs://` path to the parquet file.

- table_name:

  Character. Name for the DuckDB table.

- geom_cols:

  Character vector. Column names containing WKB geometry BLOBs to
  convert. If NULL (default), auto-detects BLOB columns with "geom" in
  their name.

## Value

Invisible NULL. Side effect: creates table in DuckDB.

## Examples

``` r
if (FALSE) { # \dontrun{
load_gcs_parquet_to_duckdb(
  con        = con,
  gcs_path   = "gs://calcofi-db/ingest/swfsc_ichthyo/grid.parquet",
  table_name = "grid")
} # }
```
