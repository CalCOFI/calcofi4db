# Integrate Ingest Outputs into Working DuckLake

Reads parquet files from multiple ingest outputs and integrates them
into the Working DuckLake. Tables from different ingests with the same
name are combined (e.g., cruise tables from different sources).
Provenance columns (`_ingest_provider`, `_ingest_dataset`) are added to
track origin.

## Usage

``` r
integrate_to_working_ducklake(
  ingests,
  gcs_bucket = "calcofi-db",
  ducklake_path = "ducklake/working"
)
```

## Arguments

- ingests:

  List of ingest result lists (from
  [`write_ingest_outputs()`](https://calcofi.io/calcofi4db/reference/write_ingest_outputs.md))
  or a tibble from
  [`list_ingest_outputs()`](https://calcofi.io/calcofi4db/reference/list_ingest_outputs.md)

- gcs_bucket:

  GCS bucket (default: "calcofi-db")

- ducklake_path:

  Path to Working DuckLake within bucket (default: "ducklake/working")

## Value

List with DuckLake path and table info

## Examples

``` r
if (FALSE) { # \dontrun{
# from targets pipeline
result <- integrate_to_working_ducklake(
  ingests = list(ingest_swfsc, ingest_bottle))

# from existing GCS ingests
existing <- list_ingest_outputs()
result <- integrate_to_working_ducklake(existing)
} # }
```
