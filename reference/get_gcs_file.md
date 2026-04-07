# Get a file from Google Cloud Storage

Downloads a file from GCS to a local path, using googleCloudStorageR or
gcloud CLI as fallback.

## Usage

``` r
get_gcs_file(gcs_path, bucket = NULL, local_path = NULL, overwrite = FALSE)
```

## Arguments

- gcs_path:

  Full GCS path (gs://bucket/path/to/file) or relative path

- bucket:

  GCS bucket name (used if gcs_path is relative)

- local_path:

  Local path to save file. If NULL, returns a temp file path

- overwrite:

  Whether to overwrite existing local file (default: FALSE)

## Value

Path to the downloaded local file

## Examples

``` r
if (FALSE) { # \dontrun{
# using full gs:// path
local_file <- get_gcs_file("gs://calcofi-files/current/bottle.csv")

# using bucket + relative path
local_file <- get_gcs_file(
  "current/calcofi/bottle/bottle.csv",
  bucket = "calcofi-files-public")
} # }
```
