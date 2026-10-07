# Check that every keyed sample sits in the cell its key names

Recomputes the cell of every `sample` position against the grid of the
connection, by the rule of
[`assign_grid_key()`](https://calcofi.io/calcofi4db/reference/assign_grid_key.md),
and compares it with the `grid_key` the row carries. This is the gate
that a grid change needs: when the cells were rebuilt
(CalCOFI/workflows#130), 84 of the 218 previous keys survived as
**names** over different polygons, so an ingest staged against the
previous grid ships keys that pass every foreign-key check and name the
wrong piece of ocean. It also means a row must be keyed by its own
position: one that inherits its key from a parent event at another
position can sit in a neighbouring cell, and fails.

## Usage

``` r
check_grid_key_assignment(
  con,
  sample_tbl = "sample",
  grid_tbl = "grid",
  halt = TRUE
)
```

## Arguments

- con:

  a DuckDB connection holding `sample_tbl` (`dataset_key`, `grid_key`,
  `longitude`, `latitude`) and `grid_tbl` (`grid_key`, `geom`)

- sample_tbl, grid_tbl:

  table names (defaults `"sample"`, `"grid"`)

- halt:

  stop when any row is `wrong` (default `TRUE`)

## Value

a data frame, one row per `dataset_key`: `n` (rows), `n_position` (with
a finite position), `n_keyed`, `n_same` (key equals the recomputed cell,
both NULL included), `n_wrong`, `n_unkeyed_in_cell` and
`n_keyed_no_position`

## Details

A row is `wrong` when it carries a key and its position falls in another
cell or in none; that fails. A row with a position inside a cell and no
key (`n_unkeyed_in_cell`) is reported, not failed: a region-pooled
dataset is ungridded by design, and the owning ingest decides.
