# Set table and column comments in DuckDB

Adds documentation comments to tables and columns in DuckDB.

## Usage

``` r
set_duckdb_comments(con, table, table_comment = NULL, column_comments = NULL)
```

## Arguments

- con:

  DuckDB connection

- table:

  Table name

- table_comment:

  Optional table-level comment

- column_comments:

  Named list of column comments

## Value

The connection (invisibly)

## Examples

``` r
if (FALSE) { # \dontrun{
set_duckdb_comments(
  con,
  table = "bottle",
  table_comment = "Bottle sample data from CalCOFI cruises",
  column_comments = list(
    cruise_id = "Unique cruise identifier",
    depth_m   = "Sample depth in meters",
    temp_c    = "Water temperature in Celsius"))
} # }
```
