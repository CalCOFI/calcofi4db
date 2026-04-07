# Read Ingest Manifest from GCS

Retrieves the manifest.json for a specific ingest workflow output.

## Usage

``` r
read_ingest_manifest(provider, dataset, gcs_bucket = "calcofi-db")
```

## Arguments

- provider:

  Data provider (e.g., "swfsc")

- dataset:

  Dataset name (e.g., "ichthyo")

- gcs_bucket:

  GCS bucket (default: "calcofi-db")

## Value

List containing manifest data

## Examples

``` r
if (FALSE) { # \dontrun{
manifest <- read_ingest_manifest(
  provider = "swfsc",
  dataset  = "ichthyo")

manifest$source_files
manifest$tables
} # }
```
