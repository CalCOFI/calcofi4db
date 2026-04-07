# Ingest a Dataset to PostgreSQL (DEPRECATED)

**\[deprecated\]**

This function is deprecated. Please use
[`ingest_dataset`](https://calcofi.io/calcofi4db/reference/ingest_dataset.md)
from `R/ducklake.R` for DuckLake workflows, or the targets pipeline with
[`ingest_to_working`](https://calcofi.io/calcofi4db/reference/ingest_to_working.md)
instead.

Legacy function to ingest a dataset into PostgreSQL database.

## Usage

``` r
ingest_dataset_pg(
  con,
  provider,
  dataset,
  dir_data,
  schema = "public",
  url_gdata = NULL,
  email = NULL,
  overwrite = FALSE
)
```

## Arguments

- con:

  Database connection

- provider:

  Data provider name

- dataset:

  Dataset name

- dir_data:

  Base directory for data

- schema:

  Database schema

- url_gdata:

  Google Drive folder URL (optional)

- email:

  Google Drive authentication email (optional)

- overwrite:

  Whether to overwrite existing tables

## Value

List with ingestion results and statistics

## Examples

``` r
if (FALSE) { # \dontrun{
result <- ingest_dataset_pg(
  con = db_connection,
  provider = "swfsc",
  dataset = "ichthyo",
  dir_data = "/path/to/data",
  schema = "public"
)
} # }
```
