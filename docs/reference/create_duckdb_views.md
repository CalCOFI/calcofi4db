# Create views from a manifest

Creates DuckDB views pointing to Parquet files defined in a manifest.

## Usage

``` r
create_duckdb_views(con, manifest)
```

## Arguments

- con:

  DuckDB connection

- manifest:

  List or data frame with table definitions

## Value

The connection object (invisibly)

## Examples

``` r
if (FALSE) { # \dontrun{
manifest <- list(
  tables = list(
    list(name = "bottle", path = "parquet/bottle.parquet"),
    list(name = "cast",   path = "parquet/cast.parquet")))

con <- get_duckdb_con()
create_duckdb_views(con, manifest)
} # }
```
