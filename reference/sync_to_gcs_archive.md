# Sync local files to GCS archive (deprecated wrapper)

Creates a new timestamped archive in GCS from local files. This is a
convenience wrapper around [sync_to_gcs(archive =
TRUE)](https://calcofi.io/calcofi4db/reference/sync_to_gcs.md).

## Usage

``` r
sync_to_gcs_archive(
  dir_csv,
  provider,
  dataset,
  gcs_bucket = "calcofi-files-public",
  archive_prefix = "archive",
  force = FALSE
)
```

## Arguments

- dir_csv:

  Local directory containing CSV files

- provider:

  Data provider

- dataset:

  Dataset name

- gcs_bucket:

  GCS bucket name

- archive_prefix:

  Archive folder prefix

- force:

  Force creation even if files match latest archive

## Value

List with `archive_timestamp`, `archive_path`, `created_new`,
`files_uploaded`.

## Examples

``` r
if (FALSE) { # \dontrun{
# preferred: use sync_to_gcs() directly
sync_to_gcs(
  local_dir  = "/path/to/csv",
  gcs_prefix = "archive",
  bucket     = "calcofi-files-public",
  archive    = TRUE,
  provider   = "swfsc",
  dataset    = "ichthyo")

# legacy wrapper (still works)
sync_to_gcs_archive(
  dir_csv  = "/path/to/csv",
  provider = "swfsc",
  dataset  = "ichthyo")
} # }
```
