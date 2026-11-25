# Assemble one core table from its per-dataset shards

UNIONs every dataset's shard into a single table. A surrogate id is
numbered **inside its dataset's block**:
`id_block * CC_ID_BLOCK_SIZE + n`, `n` being the row's rank among its
own dataset's rows under a total order of the columns. So an id changes
only when its own dataset's rows change, never because another dataset
gained or lost rows, and re-assembling unchanged shards gives the same
ids whatever order the shards or their rows arrive in.

## Usage

``` r
assemble_core_table(
  con,
  table,
  root = ".",
  id_col = NULL,
  order_by = NULL,
  parquet_dir = cc_stage_path("parquet"),
  exclude = release_excluded_datasets(root),
  id_blocks = NULL
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

  surrogate id column to number (NULL to keep the table as-is)

- order_by:

  the leading terms of the order a dataset's rows are numbered in (a
  term naming a column the table lacks, or `dataset_key`, is skipped);
  every remaining column follows as the tiebreak

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

- id_blocks:

  the registry from
  [`read_id_blocks()`](https://calcofi.io/calcofi4db/reference/read_id_blocks.md);
  read only when `id_col` is given

## Value

(invisibly) the row count written, or 0 when no shard exists
