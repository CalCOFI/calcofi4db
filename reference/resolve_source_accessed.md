# Resolve each dataset's `source_accessed`: the ingest's own stamp, else git

Reads `metadata.json` `sources[]` (written by
[`build_metadata_json()`](https://calcofi.io/calcofi4db/reference/build_metadata_json.md)
from
[`stamp_source_access()`](https://calcofi.io/calcofi4db/reference/stamp_source_access.md))
and takes the newest stamp; where an ingest has none, falls back to
[`source_accessed_from_git()`](https://calcofi.io/calcofi4db/reference/source_accessed_from_git.md).

## Usage

``` r
resolve_source_accessed(dir_parquet)
```

## Arguments

- dir_parquet:

  one or more sidecar directories (`data/parquet/{provider}_{dataset}`)

## Value

As
[`source_accessed_from_git()`](https://calcofi.io/calcofi4db/reference/source_accessed_from_git.md).
