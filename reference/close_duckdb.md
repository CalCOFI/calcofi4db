# Disconnect from DuckDB and shutdown

Properly closes a DuckDB connection and shuts down the database.

## Usage

``` r
close_duckdb(con, checkpoint = NULL)
```

## Arguments

- con:

  DuckDB connection

- checkpoint:

  Run checkpoint before closing (default: TRUE for writeable
  connections)

## Value

NULL (invisibly)

## Examples

``` r
if (FALSE) { # \dontrun{
close_duckdb(con)
} # }
```
