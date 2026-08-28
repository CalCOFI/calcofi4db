# Root sampling events with a dense integer id

One row per `sample` with no parent, numbered by `dense_rank()` over
`sample_key` so the id is deterministic across runs; carries the root's
position, time, cruise, gear and seafloor depth. Every browser object
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
