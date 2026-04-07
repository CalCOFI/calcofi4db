# Upload Frozen Release to GCS

Uploads a frozen release directory to GCS, creating required metadata
files (catalog.json, versions.json, latest.txt) and making files
publicly accessible.

## Usage

``` r
upload_frozen_release(
  release_dir,
  version,
  bucket = "calcofi-db",
  set_latest = TRUE
)
```

## Arguments

- release_dir:

  Path to local release directory (contains parquet/ subdirectory)

- version:

  Version string in format "vYYYY.MM.DD" (e.g., "v2026.03.14")

- bucket:

  GCS bucket name (default: "calcofi-db")

- set_latest:

  Logical, update latest.txt to point to this version (default: TRUE)

## Value

List with upload statistics (files uploaded, total size)

## Examples

``` r
if (FALSE) { # \dontrun{
upload_frozen_release(
  release_dir = "data/releases/v2026.03.14",
  version     = "v2026.03.14")
} # }
```
