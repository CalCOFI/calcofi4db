# Append event rows into the core `sample` dimension

`select_sql` must yield `sample_key`, `sample_type`,
`parent_sample_key`, `root_sample_key`, `dataset_key`, `grid_key`,
`cruise_key`, `latitude`, `longitude`, `datetime`, `depth_min_m`,
`depth_max_m`, `tow_type` by name; `geom` is minted here as
`ST_Point(longitude, latitude)`. `tow_type` is the net gear code
(ichthyo tow/net grains: C1/CB/CV/PV oblique/vertical, MT manta), NULL
for gears/datasets without one. Call it once per event level — a
multi-level dataset (ichthyo `site`-\>`tow`-\>`net`, bottle
`cast`-\>`bottle`) appends one arm per level, and
[`sample_arm_self()`](https://calcofi.io/calcofi4db/reference/sample_arm_self.md)
writes the single-level case for you.

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
