# Describe the objects an exported table consists of

One row per parquet object with its bytes, `sha256`, `content_hash` (row
signature of the table or of that partition's rows, provenance columns
excluded) and `since` — the first release that carried an object with
this content, looked up in the previous release's catalog.

## Usage

``` r
release_objects(
  con,
  table,
  dir_out,
  files,
  version,
  partition_by = NULL,
  prev_catalog = NULL
)
```

## Arguments

- con:

  DuckDB connection holding `table`.

- table:

  table name.

- dir_out:

  the export root (`path`'s parent for single files, or the dir).

- files:

  the tibble returned by
  [`export_release_parquet()`](https://calcofi.io/calcofi4db/reference/export_release_parquet.md).

- version:

  the release being cut.

- partition_by:

  partition column or NULL.

- prev_catalog:

  the previous release's parsed `catalog.json` (list) or NULL.

## Value

A tibble:
`table, partition_by, partition_value, rel_path, bytes, sha256, content_hash, since`.
