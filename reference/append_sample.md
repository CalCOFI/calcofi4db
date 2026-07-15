# Append event rows into the core `sample` dimension

`select_sql` must yield `sample_key`, `sample_type`,
`parent_sample_key`, `root_sample_key`, `dataset_key`, `grid_key`,
`cruise_key`, `latitude`, `longitude`, `datetime`, `depth_min_m`,
`depth_max_m` by name; `geom` is minted here as
`ST_Point(longitude, latitude)`. Prefer
[`build_sample_reference()`](https://calcofi.io/calcofi4db/reference/build_sample_reference.md)
for the central Phase-2 build; use this for per-dataset (Phase 3)
appends.

## Usage

``` r
append_sample(con, select_sql, sample_tbl = "sample")
```

## Arguments

- con:

  a DuckDB connection (open via
  [`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md))

- select_sql:

  a SELECT producing the canonical `obs` columns by name

- sample_tbl:

  target table (default `"sample"`)

## Value

(invisibly) the total row count of `sample_tbl` after the append
