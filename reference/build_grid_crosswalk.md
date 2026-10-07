# Build the `grid_crosswalk` release table

Materializes
[`grid_crosswalk()`](https://calcofi.io/calcofi4db/reference/grid_crosswalk.md)
between the previous grid and the `grid` table of the connection (the
cells this release ships, as
[`build_grid_reference()`](https://calcofi.io/calcofi4db/reference/build_grid_reference.md)
or the ichthyo ingest wrote them), so the published crosswalk describes
the published polygons. One row per overlapping pair, primary key
(`prev_grid_key`, `grid_key`); `grid_key` is a foreign key to `grid`,
`prev_grid_key` names a cell of the grid releases through v2026.10.01
carried. A cell the rebuilt grid kept as it was (the 112 beyond the
official pattern) is an identity row, with a `prev_frac` of one less the
part of it that is land under the finer coastline.

## Usage

``` r
build_grid_crosswalk(
  con,
  grid_prev = calcofi4r::cc_grid_v1,
  grid_tbl = "grid",
  tbl = "grid_crosswalk",
  crs_m = 3310,
  min_km2 = 1e-04
)
```

## Arguments

- con:

  a DuckDB connection holding `grid_tbl` with `grid_key` and a `geom`
  GEOMETRY

- grid_prev:

  the previous grid: `sf` polygons in EPSG:4326 with `grid_key` (default
  [`calcofi4r::cc_grid_v1`](https://calcofi.io/calcofi4r/reference/cc_grid_v1.html))

- grid_tbl:

  the current grid table (default `"grid"`)

- tbl:

  the table to write (default `"grid_crosswalk"`)

- crs_m, min_km2:

  passed to
  [`grid_crosswalk()`](https://calcofi.io/calcofi4db/reference/grid_crosswalk.md)

## Value

(invisibly) the crosswalk as written, a tibble

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_duckdb_con(":memory:")
build_grid_reference(con)
xw <- build_grid_crosswalk(con)
check_grid_crosswalk(con)
} # }
```
