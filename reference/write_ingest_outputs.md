# Write Ingest Workflow Outputs to GCS

Transforms data and writes parquet files + manifest to GCS ingest
folder. Each ingest workflow produces parquet files for its tables and a
manifest tracking provenance back to the source archive.

## Usage

``` r
write_ingest_outputs(
  data_info,
  provider,
  dataset,
  gcs_bucket = "calcofi-db",
  compression = "snappy"
)
```

## Arguments

- data_info:

  Output from
  [`read_csv_files()`](https://calcofi.io/calcofi4db/reference/read_csv_files.md)

- provider:

  Data provider (e.g., "swfsc")

- dataset:

  Dataset name (e.g., "ichthyo")

- gcs_bucket:

  GCS bucket for ingest outputs (default: "calcofi-db")

- compression:

  Parquet compression method (default: "snappy")

## Value

List with:

- `gcs_base`: Base GCS path for this ingest

- `parquet_paths`: Named list of GCS paths to parquet files

- `manifest_path`: GCS path to manifest.json

- `manifest`: The manifest data as a list

## Examples

``` r
if (FALSE) { # \dontrun{
d <- read_csv_files(
  provider     = "swfsc",
  dataset      = "ichthyo",
  metadata_dir = "metadata")

result <- write_ingest_outputs(
  data_info  = d,
  provider   = "swfsc",
  dataset    = "ichthyo")

# check manifest
result$manifest$tables
} # }
```
