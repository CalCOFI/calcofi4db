# List Available Ingest Outputs

Lists all providers and datasets that have ingest outputs on GCS.

## Usage

``` r
list_ingest_outputs(gcs_bucket = "calcofi-db")
```

## Arguments

- gcs_bucket:

  GCS bucket (default: "calcofi-db")

## Value

Tibble with provider, dataset, and manifest_path columns

## Examples

``` r
if (FALSE) { # \dontrun{
ingests <- list_ingest_outputs()
} # }
```
