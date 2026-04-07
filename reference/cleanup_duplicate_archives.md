# Remove duplicate archives from GCS

Compares md5 hashes across archive timestamps for a given
provider/dataset. When multiple archives have identical content, keeps
the earliest and removes the rest.

## Usage

``` r
cleanup_duplicate_archives(
  provider,
  dataset,
  gcs_bucket = "calcofi-files-public",
  archive_prefix = "archive",
  dry_run = TRUE
)
```

## Arguments

- provider:

  Data provider (e.g., "swfsc")

- dataset:

  Dataset name (e.g., "ichthyo")

- gcs_bucket:

  GCS bucket name

- archive_prefix:

  Archive folder prefix

- dry_run:

  If TRUE (default), only report what would be removed

## Value

Tibble of removed (or would-be-removed) archive timestamps

## Examples

``` r
if (FALSE) { # \dontrun{
# preview what would be removed
cleanup_duplicate_archives("swfsc", "ichthyo")

# actually remove duplicates
cleanup_duplicate_archives("swfsc", "ichthyo", dry_run = FALSE)
} # }
```
