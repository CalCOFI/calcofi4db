# Append sub-occurrence attribute rows into the core `obs_attribute` table

Generalizes the former `obs_freq`: holds any within-occurrence
attribution — length-frequency, stage-frequency, and categorical
breakdowns like seabird behavior. `select_sql` must yield `dataset_key`,
`sample_key`, `taxon_key`, `life_stage`, `measurement_type` (the
attribute, e.g. `body_length`/`stage`/ `behavior`), `bin_value` (numeric
bin / stage no.), `bin_label` (category label), `count`,
`measurement_qual` by name.

## Usage

``` r
append_obs_attribute(con, select_sql, tbl = "obs_attribute")
```

## Arguments

- con:

  a DuckDB connection (open via
  [`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md))

- select_sql:

  a SELECT producing the canonical `obs` columns by name

- tbl:

  target table (default `"obs_attribute"`)

## Value

(invisibly) the total row count of `tbl` after the append
