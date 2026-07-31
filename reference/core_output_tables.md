# Core tables an ingest writes to parquet

The core shard set, filtered to those actually present and non-empty in
`con`. Use it to drive
[`write_parquet_outputs()`](https://calcofi.io/calcofi4db/reference/write_parquet_outputs.md)
so a dataset with no `obs_attribute` (most of them) does not emit an
empty file.

## Usage

``` r
core_output_tables(con, extra = NULL)
```

## Arguments

- con:

  a DuckDB connection after the notebook's core projection has run

- extra:

  additional table names to append (shared refs the ingest owns, e.g.
  `c("grid", "cruise", "ship", "lookup")` for `swfsc_ichthyo`)

## Value

character vector of table names, core family first
