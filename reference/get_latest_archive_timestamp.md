# Get latest archive timestamp from GCS

Finds the most recent archive timestamp in the GCS bucket.

## Usage

``` r
get_latest_archive_timestamp(
  gcs_bucket = "calcofi-files-public",
  archive_prefix = "archive"
)
```

## Arguments

- gcs_bucket:

  GCS bucket name (default: "calcofi-files-public")

- archive_prefix:

  Prefix for archive folder (default: "archive")

## Value

Character string with latest timestamp, or NULL if no archives exist

## Examples

``` r
if (FALSE) { # \dontrun{
latest <- get_latest_archive_timestamp()
# "2026-02-02_121557"
} # }
```
