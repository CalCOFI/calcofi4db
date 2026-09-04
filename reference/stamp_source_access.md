# Record when an ingest read its sources

Call it at the point the bytes come down (`urls`: method `download`,
accessed now) or, for archives kept on Drive, on the files themselves
(`files`: method `file_mtime`), and hand the result to
[`build_metadata_json()`](https://calcofi.io/calcofi4db/reference/build_metadata_json.md)'s
`sources` argument so it lands in the ingest's `metadata.json` as
`sources[]`. The release then takes the newest stamp as the dataset's
`source_accessed`
([`resolve_source_accessed()`](https://calcofi.io/calcofi4db/reference/resolve_source_accessed.md)).

## Usage

``` r
stamp_source_access(files = NULL, urls = NULL)

sources_block(x)
```

## Arguments

- files:

  local files read as sources

- urls:

  URLs the sources were downloaded from

- x:

  a stamp table from `stamp_source_access()`

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html):
`source`, `method`, `accessed` (POSIXct, UTC), `bytes`.

`sources_block()`: the list
[`build_metadata_json()`](https://calcofi.io/calcofi4db/reference/build_metadata_json.md)
writes as `sources[]`.
