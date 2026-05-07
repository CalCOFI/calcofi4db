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

## Examples

``` r
if (FALSE) { # \dontrun{
grid_stats <- assign_grid_key(con, "casts")
grid_stats |> datatable()
} # }
```
