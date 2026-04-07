# Get table information from DuckDB

Retrieves schema information for all tables in a DuckDB database.

## Usage

``` r
get_duckdb_tables(con)
```

## Arguments

- con:

  DuckDB connection

## Value

Data frame with table names, column info, and row counts

## Examples

``` r
if (FALSE) { # \dontrun{
tables <- get_duckdb_tables(con)
tables
} # }
```
