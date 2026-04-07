# Compare local files with GCS archive

Compares local CSV files with a GCS archive to detect changes. Uses md5
hash as primary comparison when available, with file size as fallback
when md5 is unavailable (e.g., gcloud CLI fallback).

## Usage

``` r
compare_local_vs_archive(
  dir_csv,
  archive_timestamp,
  provider,
  dataset,
  gcs_bucket = "calcofi-files-public",
  archive_prefix = "archive"
)
```

## Arguments

- dir_csv:

  Local directory containing CSV files

- archive_timestamp:

  Archive timestamp to compare against

- provider:

  Data provider

- dataset:

  Dataset name

- gcs_bucket:

  GCS bucket name

- archive_prefix:

  Archive folder prefix

## Value

List with:

- `matches`: Logical, TRUE if local matches archive

- `local_manifest`: Tibble of local files

- `archive_manifest`: Tibble of archive files

- `added`: Files in local but not archive

- `removed`: Files in archive but not local

- `changed`: Files with different content (md5) or size

## Examples

``` r
if (FALSE) { # \dontrun{
comparison <- compare_local_vs_archive(
  dir_csv = "/path/to/csv",
  archive_timestamp = "2026-02-02_121557",
  provider = "swfsc",
  dataset = "ichthyo")

if (!comparison$matches) {
  message("Local files have changed since archive")
}
} # }
```
