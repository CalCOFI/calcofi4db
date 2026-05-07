# Export DuckDB table to Parquet

Exports a DuckDB table or query result to a Parquet file.

## Usage

``` r
duckdb_to_parquet(con, table_or_query, output, compression = "snappy")
```

## Arguments

- con:

  DuckDB connection

- table_or_query:

  Table name or SQL query

- output:

  Path for output Parquet file

- compression:

  Compression codec (default: "snappy")

## Value

Path to created Parquet file

## Examples

``` r
if (FALSE) { # \dontrun{
# export table
duckdb_to_parquet(con, "bottle", "export/bottle.parquet")

# export query result
duckdb_to_parquet(
  con,
  "SELECT * FROM bottle WHERE year >= 2020",
  "export/bottle_recent.parquet")
} # }
```
