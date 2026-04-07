# List tables with provenance in Working DuckLake

Lists all tables in the Working DuckLake along with provenance
statistics (ingestion times, source files, row counts).

## Usage

``` r
list_working_tables(con)
```

## Arguments

- con:

  DuckDB connection from
  [`get_working_ducklake()`](https://calcofi.io/calcofi4db/reference/get_working_ducklake.md)

## Value

Tibble with columns:

- `table`: Table name

- `rows`: Total row count

- `first_ingested`: Earliest ingestion timestamp

- `last_ingested`: Most recent ingestion timestamp

- `source_files`: Number of distinct source files

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_working_ducklake(read_only = TRUE)
tables <- list_working_tables(con)
tables
} # }
```
