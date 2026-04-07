# Upload a file to Google Cloud Storage

Uploads a local file to GCS.

## Usage

``` r
put_gcs_file(local_path, gcs_path, bucket = NULL, content_type = NULL)
```

## Arguments

- local_path:

  Path to the local file

- gcs_path:

  Full GCS path (gs://bucket/path) or relative path

- bucket:

  GCS bucket name (used if gcs_path is relative)

- content_type:

  MIME content type (default: auto-detect)

## Value

GCS URI of the uploaded file

## Examples

``` r
if (FALSE) { # \dontrun{
put_gcs_file("local/bottle.csv", "gs://calcofi-files/current/bottle.csv")
} # }
```
