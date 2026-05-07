# Query Working DuckLake at a point in time

Execute a query against the Working DuckLake filtering by ingestion
timestamp. This provides "time travel" capability to see data as it
existed at a past time.

## Usage

``` r
query_at_time(con, query = NULL, timestamp, table = NULL)
```

## Arguments

- con:

  DuckDB connection from
  [`get_working_ducklake()`](https://calcofi.io/calcofi4db/reference/get_working_ducklake.md)

- query:

  SQL query string. The query can reference the `_ingested_at` column,
  but this function adds a filter automatically.

- timestamp:

  Timestamp for time travel (character "YYYY-MM-DD HH:MM:SS" or
  POSIXct). Filters to rows where `_ingested_at <= timestamp`.

- table:

  Optional table name. If provided and query is NULL, queries all
  columns.

## Value

Query result as a tibble

## Details

This function provides a simple time travel mechanism by filtering on
the `_ingested_at` provenance column. For full DuckLake time travel with
transactional consistency, use native DuckLake catalog features.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_working_ducklake(read_only = TRUE)

# query larva table as of january 15th
old_data <- query_at_time(
  con       = con,
  table     = "larva",
  timestamp = "2026-01-15 00:00:00")

# custom query with time filter
results <- query_at_time(
  con       = con,
  query     = "SELECT species_id, COUNT(*) as n FROM larva GROUP BY species_id",
  timestamp = "2026-01-15")
} # }
```
