# Check that every surrogate id sits in its dataset's block

The invariant behind "an id changes only when its own dataset changes":
in each id-bearing table, a row's id is
`id_block * CC_ID_BLOCK_SIZE + n` for its own `dataset_key`, ids are
unique, and a dataset's `n` run from 1 to its row count with no gap. A
table numbered any other way (one row number over the whole table, as
through v2026.11.25) fails here.

## Usage

``` r
check_id_blocks(
  con,
  id_blocks,
  tables = c(obs = "obs_id", obs_attribute = "obs_attribute_id", sample_measurement =
    "sample_measurement_id", obs_ctd_full = "obs_id", obs_mets_full = "obs_id",
    sample_root = "root_id")
)
```

## Arguments

- con:

  a DuckDB connection holding the tables

- id_blocks:

  the registry from
  [`read_id_blocks()`](https://calcofi.io/calcofi4db/reference/read_id_blocks.md)

- tables:

  named character, table -\> id column; a table absent from the
  connection, or without both `dataset_key` and its id column, is
  skipped. `sample_root` is checked against the smaller
  [CC_ROOT_ID_BLOCK_SIZE](https://calcofi.io/calcofi4db/reference/CC_ROOT_ID_BLOCK_SIZE.md).

## Value

A data frame, one row per table x dataset:
`table, dataset_key, id_block, n, id_min, id_max, n_distinct, ok, why`
(`why` empty when `ok`).
