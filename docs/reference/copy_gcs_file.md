# Server-side copy between GCS paths

Copies a file within GCS (or between buckets) using `gcloud storage cp`.
This is a server-side operation — no data is downloaded locally.

## Usage

``` r
copy_gcs_file(src, dst)
```

## Arguments

- src:

  Full GCS source path (e.g. `"gs://bucket/src/file.csv"`)

- dst:

  Full GCS destination path (e.g. `"gs://bucket/dst/file.csv"`)

## Value

Destination GCS path (invisibly)
