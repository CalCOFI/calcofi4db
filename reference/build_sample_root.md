# Root sampling events with a dense integer id

One row per `sample` with no parent, numbered by `dense_rank()` over
`sample_key` so the id is deterministic across runs; carries the root's
position, time, cruise, gear, seafloor depth and — last — `hex7`, the
root's own H3 cell exactly as `sample` holds it. Every browser object
joins on `root_id`; `root_sample_key` is what it stands for.

## Usage

``` r
build_sample_root(con, tbl = "sample_root")
```

## Arguments

- con:

  DuckDB connection holding `sample`.

- tbl:

  name of the table to (re)create.

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
