# Ingest CSV data to PostgreSQL database (DEPRECATED)

**\[deprecated\]**

This function is deprecated. Please use
[`ingest_to_working`](https://calcofi.io/calcofi4db/reference/ingest_to_working.md)
instead for ingesting data into the DuckLake database with automatic
provenance tracking.

Loads transformed data into a PostgreSQL database with proper metadata.

## Usage

``` r
ingest_csv_to_db(
  con,
  schema,
  transformed_data,
  d_flds_rd,
  d_gdir_data = NULL,
  workflow_info,
  overwrite = FALSE
)
```

## Arguments

- con:

  Database connection

- schema:

  Database schema

- transformed_data:

  Transformed data from transform_data()

- d_flds_rd:

  Field redefinition data frame

- d_gdir_data:

  Google Drive metadata (optional)

- workflow_info:

  Workflow information (name, URL, etc.)

- overwrite:

  Whether to overwrite existing tables

## Value

Data frame with ingestion statistics

## Examples

``` r
if (FALSE) { # \dontrun{
stats <- ingest_csv_to_db(
  con = db_connection,
  schema = "public",
  transformed_data = transformed_data,
  d_flds_rd = data_info$d_flds_rd,
  d_gdir_data = data_info$d_gdir_data,
  workflow_info = data_info$workflow_info
)
} # }
```
