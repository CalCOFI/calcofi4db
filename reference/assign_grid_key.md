# Assign Grid Key via Spatial Join

Uses DuckDB spatial SQL to add a `grid_key` column to a table by
intersecting its geometry with a grid table. Returns a summary of how
many rows fell inside vs outside the grid.

## Usage

``` r
assign_grid_key(con, table, geom_col = "geom", grid_table = "grid")
```

## Arguments

- con:

  DBI connection to DuckDB

- table:

  Character. Table name to update.

- geom_col:

  Character. Geometry column in `table` (default: "geom").

- grid_table:

  Character. Grid table name (default: "grid").

## Value

Data frame with columns `status` (in_grid / not_in_grid) and `n`.

## Details

The rule, which
[`calcofi4r::cc_grid_key()`](https://calcofi.io/calcofi4r/reference/cc_grid_key.html)
applies identically in R: a position takes the cell whose polygon it
intersects, on longitude/latitude as planar coordinates; a position on
an edge shared by several cells takes the key that sorts first in byte
order (`min(grid_key)`); a position in no cell (on land, outside the
grid, a NULL geometry) gets NULL. Through calcofi4db 4.17.2 the tie was
`LIMIT 1` with no order, so a position on a shared edge could key to
either cell from one run to the next.

## Examples

``` r
if (FALSE) { # \dontrun{
grid_stats <- assign_grid_key(con, "casts")
grid_stats |> datatable()
} # }
```
