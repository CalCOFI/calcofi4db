# Read Ingest Parquet Table from GCS

Downloads and reads a parquet table from an ingest output.

## Usage

``` r
read_ingest_parquet(provider, dataset, table, gcs_bucket = "calcofi-db")
```

## Arguments

- provider:

  Data provider (e.g., "swfsc")

- dataset:

  Dataset name (e.g., "ichthyo")

- table:

  Table name (e.g., "cruise")

- gcs_bucket:

  GCS bucket (default: "calcofi-db")

## Value

Data frame (tibble) of the table

## Examples

``` r
if (FALSE) { # \dontrun{
cruise <- read_ingest_parquet(
  provider = "swfsc",
  dataset  = "ichthyo",
  table    = "cruise")
} # }
```
