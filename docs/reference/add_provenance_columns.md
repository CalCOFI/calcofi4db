# Add provenance columns to a data frame

Helper function to add provenance tracking columns to a data frame
before ingestion. Called internally by
[`ingest_to_working()`](https://calcofi.io/calcofi4db/reference/ingest_to_working.md).

## Usage

``` r
add_provenance_columns(
  data,
  source_file,
  source_row_start = 1,
  source_uuid_col = NULL
)
```

## Arguments

- data:

  Data frame to modify

- source_file:

  Path to original CSV file in archive (e.g.,
  "archive/2026-02-02_121557/swfsc/ichthyo/larva.csv")

- source_row_start:

  Starting row number (default: 1, typically 2 to skip header)

- source_uuid_col:

  Column name containing original UUIDs (optional). If provided, values
  are copied to `_source_uuid` column.

## Value

Data frame with added provenance columns:

- `_source_file` (character): Path to original CSV

- `_source_row` (integer): Row number in source file

- `_source_uuid` (character): Original record UUID if available

- `_ingested_at` (POSIXct): When row was ingested (UTC)

## Examples

``` r
if (FALSE) { # \dontrun{
data <- tibble::tibble(x = 1:3, y = letters[1:3])
data_prov <- add_provenance_columns(
  data        = data,
  source_file = "archive/2026-02-02_121557/swfsc/ichthyo/test.csv",
  source_row_start = 2)  # skip header

# with source uuid column
data <- tibble::tibble(x = 1:3, uuid = c("a1", "b2", "c3"))
data_prov <- add_provenance_columns(
  data            = data,
  source_file     = "test.csv",
  source_uuid_col = "uuid")
} # }
```
