# Get manifest for a specific date

Retrieves the manifest JSON from GCS for a given sync timestamp. The
manifest contains metadata about all files in the archive snapshot.

## Usage

``` r
get_manifest(date = "latest", bucket = "public")
```

## Arguments

- date:

  Date string in format "YYYY-MM-DD_HHMMSS" or "latest" (default)

- bucket:

  Bucket type: "public" or "private" (default: "public")

## Value

List containing manifest data (generated_at, sync_timestamp,
archive_path, files)

## Examples

``` r
if (FALSE) { # \dontrun{
manifest <- get_manifest()
manifest <- get_manifest("2026-02-01_143059", "public")
} # }
```
