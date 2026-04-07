# Sync local files to GCS, skipping unchanged files

Compares local files against GCS using checksums (CRC32C \> MD5 \>
size). Only uploads files that are new or changed.

## Usage

``` r
sync_to_gcs(
  local_dir,
  gcs_prefix,
  bucket,
  pattern = NULL,
  exclude = NULL,
  delete_stale = FALSE,
  log_to_gcs = FALSE,
  archive = FALSE,
  provider = NULL,
  dataset = NULL,
  verbose = TRUE
)
```

## Arguments

- local_dir:

  Directory containing files to upload

- gcs_prefix:

  GCS destination prefix (e.g. "ingest/swfsc_ichthyo" or "archive" for
  archive mode)

- bucket:

  GCS bucket name

- pattern:

  Regex to filter local files (default: NULL = all files)

- exclude:

  Character vector of glob patterns to skip (e.g.
  `c(".DS_Store", "*.tmp")`). Applied to relative file paths.

- delete_stale:

  If TRUE, delete GCS files that no longer exist locally. Default FALSE.
  Ignored in archive mode.

- log_to_gcs:

  If TRUE, write a timestamped JSON action log to
  `gs://{bucket}/{gcs_prefix}/_logs/sync_YYYY-MM-DD_HHMMSS.json`.

- archive:

  If TRUE, use archive mode: creates a timestamped immutable snapshot.
  Requires `provider` and `dataset`.

- provider:

  Data provider (required when `archive = TRUE`)

- dataset:

  Dataset name (required when `archive = TRUE`)

- verbose:

  Print per-file status messages (default: TRUE)

## Value

In mirror mode: tibble with columns `file`, `action`, `size`. In archive
mode: list with `archive_timestamp`, `archive_path`, `created_new`,
`files_uploaded`.

## Details

Two modes:

- **Mirror mode** (default): syncs `local_dir/` to
  `gs://{bucket}/{gcs_prefix}/`. Optionally deletes stale GCS files.

- **Archive mode** (`archive = TRUE`): creates a timestamped immutable
  snapshot at
  `gs://{bucket}/{gcs_prefix}/{timestamp}/{provider}/{dataset}/`.
  Timestamp derived from max file mtime for reproducibility. Skips
  upload if files match the latest existing archive.

## Examples

``` r
if (FALSE) { # \dontrun{
# mirror mode: sync parquet outputs
sync_to_gcs(
  local_dir  = "data/parquet/swfsc_ichthyo",
  gcs_prefix = "ingest/swfsc_ichthyo",
  bucket     = "calcofi-db")

# mirror mode: full GD backup with stale cleanup
sync_to_gcs(
  local_dir    = "~/My Drive/projects/calcofi/data-public",
  gcs_prefix   = "_sync",
  bucket       = "calcofi-files-public",
  delete_stale = TRUE,
  log_to_gcs   = TRUE,
  exclude      = c(".DS_Store", "*.tmp"))

# archive mode: timestamped snapshot of source CSVs
sync_to_gcs(
  local_dir  = "path/to/csv",
  gcs_prefix = "archive",
  bucket     = "calcofi-files-public",
  archive    = TRUE,
  provider   = "swfsc",
  dataset    = "ichthyo")
} # }
```
