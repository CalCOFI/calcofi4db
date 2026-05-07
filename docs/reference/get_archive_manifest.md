# Get archive manifest (file metadata)

Retrieves metadata (size, md5, path) for all CSV files in a GCS archive.
The md5 hash is converted from GCS base64 encoding to hex for comparison
with [`tools::md5sum()`](https://rdrr.io/r/tools/md5sum.html).

## Usage

``` r
get_archive_manifest(
  archive_timestamp,
  provider,
  dataset,
  gcs_bucket = "calcofi-files-public",
  archive_prefix = "archive"
)
```

## Arguments

- archive_timestamp:

  Archive timestamp (e.g., "2026-02-02_121557")

- provider:

  Data provider (e.g., "swfsc")

- dataset:

  Dataset name (e.g., "ichthyo")

- gcs_bucket:

  GCS bucket name

- archive_prefix:

  Archive folder prefix

## Value

Tibble with columns: name, size, md5, gcs_path

## Examples

``` r
if (FALSE) { # \dontrun{
manifest <- get_archive_manifest(
  archive_timestamp = "2026-02-02_121557",
  provider = "swfsc",
  dataset = "ichthyo")
} # }
```
