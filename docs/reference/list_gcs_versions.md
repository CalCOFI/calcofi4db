# List versions of a file in GCS archive

Finds all archived versions of a file in the calcofi-files bucket.

## Usage

``` r
list_gcs_versions(path, bucket = "calcofi-files-public")
```

## Arguments

- path:

  Relative path to the file (e.g., "calcofi/bottle/bottle.csv")

- bucket:

  GCS bucket name (default: "calcofi-files")

## Value

Data frame with columns: version_date, gcs_path, size, updated

## Examples

``` r
if (FALSE) { # \dontrun{
versions <- list_gcs_versions("calcofi/bottle/bottle.csv")
} # }
```
