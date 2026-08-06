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
  parquet_dir = cc_stage_path("parquet"),
  exclude = release_excluded_datasets(root)
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

  directory holding the per-dataset output dirs. Defaults to the local
  staging root (see
  [`cc_stage_dir()`](https://calcofi.io/calcofi4db/reference/cc_stage_dir.md)),
  where the bulk parquet lives; an absolute path is used as-is, a
  relative one is resolved against `root`. The JSON sidecars stay in the
  repo and are found separately.

- exclude:

  dataset dir names to skip (see
  [`core_shard_paths()`](https://calcofi.io/calcofi4db/reference/core_shard_paths.md))

## Value

(invisibly) the row count written, or 0 when no shard exists
