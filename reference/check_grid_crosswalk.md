# Gate the grid crosswalk

What must hold for `grid_crosswalk` to be the map between the two grids:
every previous cell appears, every current cell appears, no pair
repeats, every key is a cell of its grid, and no previous cell's shares
sum to more than one (they would only if two current cells overlapped,
and the current grid is a partition).

## Usage

``` r
check_grid_crosswalk(
  con,
  grid_prev = calcofi4r::cc_grid_v1,
  tbl = "grid_crosswalk",
  grid_tbl = "grid",
  min_cover = 0.95,
  tol = 1e-06,
  halt = TRUE
)
```

## Arguments

- con:

  a DuckDB connection holding `tbl` and `grid_tbl`

- grid_prev:

  the previous grid (default
  [`calcofi4r::cc_grid_v1`](https://calcofi.io/calcofi4r/reference/cc_grid_v1.html));
  only its keys are read

- tbl, grid_tbl:

  the crosswalk and the current grid tables

- min_cover:

  the smallest summed share a cell may have (default 0.95; measured on
  the rebuilt grid: 0.978 for a previous cell, 0.982 for a current one)

- tol:

  tolerance on a sum differing from one (default 1e-6)

- halt:

  stop on a failure (default `TRUE`); `FALSE` returns the report
  regardless

## Value

a data frame, one row per cell of either grid: `side` (`"prev"` or
`"grid"`), `grid_key`, `n` (cells of the other grid it overlaps), `frac`
(summed share), `main_key` (the other grid's cell holding its largest
share), `main_frac` and `status` (`"ok"`, `"partial"`, `"overlapped"`,
or a failure: `"no overlap"`, `"under min_cover"`, `"over one"`)

## Details

Three things are reported rather than failed, because they are facts
about the two grids and not errors in the map. A cell whose shares sum
to **less** than one, down to `min_cover`, is `"partial"`: the remainder
is area the other grid does not cover (the previous cells were clipped
by a coarser coastline, so part of one can be land now). Below
`min_cover` it fails, since a cell most of which has no counterpart is a
hole. A **current** cell whose shares sum to more than one is
`"overlapped"`: previous cells overlap each other there (the previous
grid was assembled in `+proj=calcofi` and glued by hand along line 93.3,
and is not an exact partition in longitude/latitude; seven current cells
either side of that boundary are covered twice over at most 1e-4 of
their area).
