# Ingest data to Working DuckLake

Ingests a data frame into the Working DuckLake with automatic provenance
tracking. Adds `_source_file`, `_source_row`, `_source_uuid`, and
`_ingested_at` columns to track data lineage.

## Usage

``` r
ingest_to_working(
  con,
  data,
  table,
  source_file,
  source_uuid_col = NULL,
  source_row_start = 1,
  mode = "append",
  upsert_keys = NULL
)
```

## Arguments

- con:

  DuckDB connection from
  [`get_working_ducklake()`](https://calcofi.io/calcofi4db/reference/get_working_ducklake.md)

- data:

  Data frame to ingest

- table:

  Target table name

- source_file:

  Path to original CSV file in Archive (for provenance). Should be the
  full path including archive timestamp (e.g.,
  "archive/2026-02-02_121557/swfsc/ichthyo/larva.csv")

- source_uuid_col:

  Column name containing original UUIDs (optional). If provided, values
  are stored in `_source_uuid` for tracing back to source records.

- source_row_start:

  Starting row number in source file (default: 1)

- mode:

  Insert mode: "append", "replace", or "upsert" (default: "append")

  - `append`: Add rows to existing table

  - `replace`: Drop and recreate table

  - `upsert`: Update existing rows, insert new rows (requires
    `upsert_keys`)

- upsert_keys:

  Character vector of column names to use as keys for upsert mode

## Value

Tibble with ingestion statistics:

- `table`: Table name

- `mode`: Insert mode used

- `rows_input`: Number of rows in input data

- `rows_after`: Number of rows in table after ingestion

- `ingested_at`: Timestamp of ingestion

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_working_ducklake()

larvae_data <- readr::read_csv("larvae.csv")
stats <- ingest_to_working(
  con         = con,
  data        = larvae_data,
  table       = "larva",
  source_file = "archive/2026-02-02_121557/swfsc/ichthyo/larva.csv",
  source_uuid_col = "larva_uuid",
  mode        = "replace")

# append new data
new_data <- readr::read_csv("larvae_update.csv")
stats <- ingest_to_working(
  con         = con,
  data        = new_data,
  table       = "larva",
  source_file = "archive/2026-03-01_121557/swfsc/ichthyo/larva.csv",
  mode        = "append")
} # }
```
