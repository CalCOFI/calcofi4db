# Ingest Dataset into Working DuckLake

High-level function to ingest all tables from a dataset into the Working
DuckLake. This wraps
[`transform_data()`](https://calcofi.io/calcofi4db/reference/transform_data.md)
and
[`ingest_to_working()`](https://calcofi.io/calcofi4db/reference/ingest_to_working.md)
into a single operation with proper provenance tracking.

## Usage

``` r
ingest_dataset(con, d, mode = "replace", verbose = TRUE)
```

## Arguments

- con:

  DuckDB connection from
  [`get_working_ducklake()`](https://calcofi.io/calcofi4db/reference/get_working_ducklake.md)

- d:

  Data object from
  [`read_csv_files()`](https://calcofi.io/calcofi4db/reference/read_csv_files.md)

- mode:

  Insert mode: "replace" (default) or "append"

- verbose:

  Print progress messages (default: TRUE)

## Value

Tibble with ingestion statistics for each table:

- `tbl`: Original table name

- `tbl_new`: New table name after redefinition

- `gcs_path`: Source file path for provenance

- `rows_input`: Number of rows ingested

- `rows_after`: Total rows in table after ingestion

- `ingested_at`: Timestamp of ingestion

## Examples

``` r
if (FALSE) { # \dontrun{
# read and ingest dataset
d <- read_csv_files(
  provider     = "swfsc",
  dataset      = "ichthyo",
  dir_data     = "~/My Drive/projects/calcofi/data-public",
  metadata_dir = "metadata")

con <- get_working_ducklake()
stats <- ingest_dataset(con, d, mode = "replace")
save_working_ducklake(con)
close_duckdb(con)
} # }
```
