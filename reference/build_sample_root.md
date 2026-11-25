# Root sampling events with an integer id

One row per `sample` with no parent. With `id_blocks` (the release
passes
[`read_id_blocks()`](https://calcofi.io/calcofi4db/reference/read_id_blocks.md)),
`root_id` is numbered inside its dataset's block,
`id_block * CC_ROOT_ID_BLOCK_SIZE + n`, `n` ranking the dataset's own
roots by time and then `sample_key`: a root's id changes only when its
own dataset's roots change. Through v2026.11.25 it was one
`dense_rank()` over every `sample_key`, so a dataset that gained a cast
renumbered the roots of every dataset sorted after it, and with them
every row of `obs_bio`, `obs_env` and `sample_spatial`. Without
`id_blocks` that dense rank is still what is built (a fixture, a dry
run); a release gates on
[`check_id_blocks()`](https://calcofi.io/calcofi4db/reference/check_id_blocks.md).
The table carries the root's position, time, cruise, gear, seafloor
depth, `hex7` — the root's own H3 cell exactly as `sample` holds it —
and, last, `data_stage`. Every browser object joins on `root_id`;
`root_sample_key` is what it stands for.

## Usage

``` r
build_sample_root(con, tbl = "sample_root", id_blocks = NULL)
```

## Arguments

- con:

  DuckDB connection holding `sample`.

- tbl:

  name of the table to (re)create.

- id_blocks:

  the registry from
  [`read_id_blocks()`](https://calcofi.io/calcofi4db/reference/read_id_blocks.md),
  or NULL for one dense rank over `sample_key` (not for a release).

## Value

Invisibly, the row count.

## Details

`hex7` is **carried**, never recomputed here, so `sample.hex7` and
`sample_root.hex7` cannot disagree for a root: stamp `sample` with
[`add_sample_hex7()`](https://calcofi.io/calcofi4db/reference/add_sample_hex7.md)
first. On a `sample` without the column `hex7` is `NULL` (like
`seafloor_depth_m`) and a message says so;
[`check_sample_hex7()`](https://calcofi.io/calcofi4db/reference/check_sample_hex7.md)
fails such a release.

`data_stage` is the root sample's own `sample.data_stage`, as the
dataset's ingest wrote it (`final`, `preliminary_with_bottle`,
`preliminary_without_bottle` on the CTD casts) and `NULL` where the
dataset records none; it is the trailing column, so every earlier column
keeps its position. It lets a reader of `sample_root` say that a
cruise's data are preliminary without fetching `sample`. On a `sample`
without the column it is `NULL`, typed `VARCHAR`.
