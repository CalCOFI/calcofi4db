# Export a DuckDB Table or Query to Parquet

Uses DuckDB's native `COPY TO` which handles GEOMETRY columns (as WKB),
compression, and hive partitioning. Preferred over
[`arrow::write_parquet()`](https://arrow.apache.org/docs/r/reference/write_parquet.html)
for any table that may contain geometry.

## Usage

``` r
export_parquet(con, table, path, compression = "snappy")
```

## Arguments

- con:

  DuckDB connection

- table:

  Table name or SQL query (wrapped in parentheses for queries)

- path:

  Output parquet file path

- compression:

  Compression codec (default: "snappy")

## Value

Invisibly returns the output path

## Examples

``` r
if (FALSE) { # \dontrun{
# export a table
export_parquet(con, "ship", "output/ship.parquet")

# export a query
export_parquet(con, "(SELECT * FROM ship WHERE ship_key = 'ZZ')",
  "output/ship_new.parquet")
} # }
```
