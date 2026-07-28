# Assemble one core table from its per-dataset shards

UNIONs every dataset's shard into a single table. Surrogate ids are
renumbered globally after the union (each ingest numbers from 1 within
its own shard, so the raw ids collide across datasets).

## Usage

``` r
assemble_core_table(
  con,
  table,
  root = ".",
  id_col = NULL,
  order_by = NULL,
  parquet_dir = "data/parquet"
)
```

## Arguments

- con:

  a DuckDB connection

- table:

  core table name

- root:

  workflows repo root

- id_col:

  surrogate id column to renumber globally (NULL to keep as-is)

- order_by:

  optional ORDER BY used when renumbering, so ids are stable across
  re-runs of unchanged data

- parquet_dir:

  directory holding the per-dataset output dirs

## Value

(invisibly) the row count written, or 0 when no shard exists
