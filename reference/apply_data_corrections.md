# Apply Data Corrections

Applies known data corrections to the database. This function should be
updated as new corrections are identified by data managers.

## Usage

``` r
apply_data_corrections(con, verbose = TRUE)
```

## Arguments

- con:

  DuckDB connection

- verbose:

  Print correction messages (default: TRUE)

## Value

Invisibly returns the connection

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_duckdb_con()
apply_data_corrections(con)
} # }
```
