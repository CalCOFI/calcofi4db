# Download archive to local directory

Downloads CSV files from a GCS archive to a local directory.

## Usage

``` r
download_archive(
  archive_timestamp,
  provider,
  dataset,
  local_dir = NULL,
  gcs_bucket = "calcofi-files-public",
  archive_prefix = "archive"
)
```

## Arguments

- archive_timestamp:

  Archive timestamp

- provider:

  Data provider

- dataset:

  Dataset name

- local_dir:

  Local directory to download to (default: temp directory)

- gcs_bucket:

  GCS bucket name

- archive_prefix:

  Archive folder prefix

## Value

Path to local directory containing downloaded files

## Examples

``` r
if (FALSE) { # \dontrun{
local_dir <- download_archive(
  archive_timestamp = "2026-02-02_121557",
  provider = "swfsc",
  dataset = "ichthyo")
} # }
```
