# Install and load DuckDB extension

Installs (if needed) and loads a DuckDB extension.

## Usage

``` r
load_duckdb_extension(con, extension, from = NULL)
```

## Arguments

- con:

  DuckDB connection

- extension:

  Extension name (e.g., "spatial", "h3", "httpfs")

- from:

  Optional source ("community" or specific URL)

## Value

The connection (invisibly)

## Examples

``` r
if (FALSE) { # \dontrun{
# install community extension
load_duckdb_extension(con, "h3", from = "community")

# install core extension
load_duckdb_extension(con, "spatial")
} # }
```
