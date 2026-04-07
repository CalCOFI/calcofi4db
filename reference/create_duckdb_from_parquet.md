# Create DuckDB from Parquet files

Creates or updates a DuckDB database from a collection of Parquet files.

## Usage

``` r
create_duckdb_from_parquet(
  parquet_files,
  db_path = ":memory:",
  table_names = NULL
)
```

## Arguments

- parquet_files:

  Named list or vector of Parquet file paths

- db_path:

  Path for DuckDB file (default: ":memory:")

- table_names:

  Optional table names (default: derived from file names)

## Value

DuckDB connection with loaded tables

## Examples

``` r
if (FALSE) { # \dontrun{
# create from multiple parquet files
con <- create_duckdb_from_parquet(
  c("parquet/bottle.parquet", "parquet/cast.parquet"),
  db_path = "calcofi.duckdb")

# with explicit table names
con <- create_duckdb_from_parquet(
  parquet_files = c(
    bottle = "parquet/bottle.parquet",
    cast   = "parquet/cast.parquet"),
  db_path = "calcofi.duckdb")
} # }
```
