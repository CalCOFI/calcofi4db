# List files in a GCS bucket/prefix

Lists objects in a GCS bucket with optional prefix filter.

## Usage

``` r
list_gcs_files(bucket, prefix = NULL, recursive = TRUE)
```

## Arguments

- bucket:

  GCS bucket name

- prefix:

  Path prefix to filter results

- recursive:

  Whether to list recursively (default: TRUE)

## Value

Data frame with columns: name, size, updated, md5Hash

## Examples

``` r
if (FALSE) { # \dontrun{
# list all files in current/
files <- list_gcs_files("calcofi-files", prefix = "current/")

# list bottle files
files <- list_gcs_files(
  "calcofi-files",
  prefix = "current/calcofi/bottle/")
} # }
```
