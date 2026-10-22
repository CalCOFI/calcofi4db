# Buckets anyone can read

The buckets the pipeline writes that grant `allUsers` read: the release
and staging bucket and the public source mirror.
[`sync_to_gcs()`](https://calcofi.io/calcofi4db/reference/sync_to_gcs.md)
keeps an unreleased dataset out of them.

## Usage

``` r
CC_PUBLIC_BUCKETS
```
