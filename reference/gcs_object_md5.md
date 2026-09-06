# The MD5 of a GCS object, as GCS reports it (base64), or NA when it does not exist

One metadata read through `gcloud storage objects describe`; the value
is what `local_md5_base64()` produces for the same bytes, so the pair
says whether an upload would change anything. A composite object carries
no md5Hash and reads as NA, which means "upload" — the safe side.

## Usage

``` r
gcs_object_md5(gcs_uri)

local_md5_base64(path)
```

## Arguments

- gcs_uri:

  a `gs://bucket/path`

- path:

  a local file

## Value

a base64 MD5 string, or `NA_character_`

`local_md5_base64()`: the file's MD5 in the base64 form GCS uses.
