# Upload a file to Google Cloud Storage

Uploads a local file to GCS.

## Usage

``` r
put_gcs_file(
  local_path,
  gcs_path,
  bucket = NULL,
  content_type = NULL,
  skip_unchanged = TRUE
)
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

- skip_unchanged:

  If TRUE (the default), an object that already exists at `gcs_path`
  with the same MD5 as the local file is not uploaded again — the check
  is a metadata read
  ([`gcs_object_md5()`](https://calcofi.io/calcofi4db/reference/gcs_object_md5.md)),
  the upload is the bytes. A publisher re-run over a frozen release must
  cost a hash comparison, not a multi-GB transfer (2026-09-06).

## Value

GCS URI of the uploaded (or already-identical) file

## Examples

``` r
if (FALSE) { # \dontrun{
put_gcs_file("local/bottle.csv", "gs://calcofi-files/current/bottle.csv")
} # }
```
