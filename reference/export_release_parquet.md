# Deterministic parquet export of one released table

Writes `SELECT <non-provenance cols> FROM table ORDER BY <order_by>`
with the pinned writer options
([CC_PARQUET_WRITER](https://calcofi.io/calcofi4db/reference/CC_PARQUET_WRITER.md))
and a single writer thread, so the same rows always produce the same
bytes. Refuses to write if `order_by` is not a unique key of the table.

## Usage

``` r
export_release_parquet(
  con,
  table,
  path,
  order_by,
  partition_by = NULL,
  writer = CC_PARQUET_WRITER,
  strip_provenance = TRUE
)
```

## Arguments

- con:

  DuckDB connection.

- table:

  table (or view) name.

- path:

  output file (single) or directory (partitioned).

- order_by:

  character vector of ORDER BY terms (may carry `NULLS LAST`).

- partition_by:

  optional partition column (Hive layout, one file per value).

- writer:

  list of writer options; see
  [CC_PARQUET_WRITER](https://calcofi.io/calcofi4db/reference/CC_PARQUET_WRITER.md).

- strip_provenance:

  drop `_source_*` / `_ingested_at` style columns.

## Value

Invisibly, a tibble of files written: `rel_path`, `bytes`.
