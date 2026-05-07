# Add Point Geometry Column to a DuckDB Table

Uses DuckDB spatial SQL to add a GEOMETRY column from longitude/latitude
columns. No sf dependency needed.

## Usage

``` r
add_point_geom(
  con,
  table,
  lon_col = "lon_dec",
  lat_col = "lat_dec",
  geom_col = "geom"
)
```

## Arguments

- con:

  DBI connection to DuckDB

- table:

  Character. Table name.

- lon_col:

  Character. Longitude column name (default: "lon_dec").

- lat_col:

  Character. Latitude column name (default: "lat_dec").

- geom_col:

  Character. Geometry column name to create (default: "geom").

## Value

Invisible NULL. Side effect: adds geometry column to table.

## Examples

``` r
if (FALSE) { # \dontrun{
add_point_geom(con, "casts")
add_point_geom(con, "site", lon_col = "longitude", lat_col = "latitude")
} # }
```
