# Get a DuckDB connection

Creates a connection to a DuckDB database, either local file or
in-memory. Optionally downloads from GCS if path starts with gs://.

## Usage

``` r
get_duckdb_con(path = ":memory:", read_only = FALSE, config = list())
```

## Arguments

- path:

  Path to DuckDB file, GCS path, or ":memory:" for in-memory

- read_only:

  Open database in read-only mode (default: FALSE)

- config:

  Named list of DuckDB configuration options (default: empty list)

## Value

DuckDB connection object

## Examples

``` r
if (FALSE) { # \dontrun{
# in-memory database
con <- get_duckdb_con()

# local file
con <- get_duckdb_con("data/calcofi.duckdb")

# from GCS (downloads first)
con <- get_duckdb_con("gs://calcofi-db/duckdb/calcofi.duckdb")

# read-only with custom config
con <- get_duckdb_con(
  "calcofi.duckdb",
  read_only = TRUE,
  config = list(threads = 4))
} # }
```
