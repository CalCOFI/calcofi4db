# Delete all objects under a GCS prefix

Recursively deletes all objects matching the given prefix in a GCS
bucket. Uses `gcloud storage rm -r` under the hood. Always requires
explicit confirmation via `dry_run = FALSE`.

## Usage

``` r
delete_gcs_prefix(prefix, bucket = "calcofi-db", dry_run = TRUE)
```

## Arguments

- prefix:

  GCS prefix to delete (e.g., "ingest/old_dataset/")

- bucket:

  GCS bucket name (default: "calcofi-db")

- dry_run:

  If TRUE (default), only list what would be deleted

## Value

Tibble with prefix and action (would_delete / deleted)
