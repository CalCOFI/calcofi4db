# Upload Parquet file to GCS

Convenience function to upload a Parquet file to the calcofi-db bucket.

## Usage

``` r
upload_parquet(local_path, gcs_path, bucket = "calcofi-db")
```

## Arguments

- local_path:

  Path to local Parquet file

- gcs_path:

  Relative path in calcofi-db bucket (e.g., "parquet/bottle.parquet")

- bucket:

  GCS bucket name (default: "calcofi-db")

## Value

GCS URI of uploaded file

## Examples

``` r
if (FALSE) { # \dontrun{
upload_parquet("local/bottle.parquet", "parquet/bottle.parquet")
} # }
```
