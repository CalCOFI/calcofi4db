# Clean up obsolete GCS directories from dataset renames

Removes known-obsolete GCS prefixes left over from dataset renames and
the retired Working DuckLake monolith.

## Usage

``` r
cleanup_gcs_obsolete(bucket = "calcofi-db", dry_run = TRUE)
```

## Arguments

- bucket:

  GCS bucket name (default: "calcofi-db")

- dry_run:

  If TRUE (default), only list what would be deleted

## Value

Tibble with prefix and action
