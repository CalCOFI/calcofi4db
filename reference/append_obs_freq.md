# Append (bin, count) rows into the core `obs_freq` table

`select_sql` must yield `dataset_key`, `sample_key`, `taxon_id`,
`life_stage`, `measurement_type` (the binned attribute, e.g.
`body_length`/`stage`), `bin_value`, `bin_label`, `count`,
`measurement_qual` by name.

## Usage

``` r
append_obs_freq(con, select_sql, tbl = "obs_freq")
```

## Arguments

- con:

  a DuckDB connection (open via
  [`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md))

- select_sql:

  a SELECT producing the canonical `obs` columns by name

- tbl:

  target table (default `"obs_freq"`)

## Value

(invisibly) the total row count of `tbl` after the append
