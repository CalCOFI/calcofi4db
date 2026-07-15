# Append occurrence-headline rows into the core `obs` table

Wraps a caller-supplied projection `select_sql` (which must yield the
canonical `obs` columns *by name* — `realm`, `dataset_key`,
`sample_key`, `grid_key`, `cruise_key`, `latitude`, `longitude`,
`datetime`, `depth_min_m`, `depth_max_m`, `taxon_id`, `life_stage`,
`measurement_type`, `measurement_value`, `measurement_qual`,
`measurement_prec`), mints a surrogate `obs_id` (offset from the current
max so repeated calls stay unique within one connection) and computes
`hex_id` at H3 resolution `res_max`. The same helper serves the central
Phase-2 materialization (`release_database.qmd`) and each per-dataset
ingest (Phase 3); release assembly renumbers `obs_id` globally across
the reassembled shards.

## Usage

``` r
append_obs(con, select_sql, obs_tbl = "obs", res_max = CC_H3_RES_MAX)
```

## Arguments

- con:

  a DuckDB connection (open via
  [`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md))

- select_sql:

  a SELECT producing the canonical `obs` columns by name

- obs_tbl:

  target table name (`"obs"`, or `"obs_ctd_full"` for full CTD)

- res_max:

  finest H3 resolution stored in `hex_id`

## Value

(invisibly) the total row count of `obs_tbl` after the append
