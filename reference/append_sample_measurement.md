# Append event-level (effort) rows into the core `sample_measurement` table

`select_sql` must yield `sample_key`, `dataset_key`, `measurement_type`,
`measurement_value`, `measurement_qual` by name.

## Usage

``` r
append_sample_measurement(con, select_sql, tbl = "sample_measurement")
```

## Arguments

- con:

  a DuckDB connection (open via
  [`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md))

- select_sql:

  a SELECT producing the canonical `obs` columns by name

- tbl:

  target table (default `"sample_measurement"`)

## Value

(invisibly) the total row count of `tbl` after the append
