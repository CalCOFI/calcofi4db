# Append event rows into the core `sample` dimension

`select_sql` is bound **positionally**, so it must yield either the 15
columns of the base contract — `sample_key`, `sample_type`,
`parent_sample_key`, `root_sample_key`, `dataset_key`, `grid_key`,
`site_key`, `cruise_key`, `order_occ`, `latitude`, `longitude`,
`datetime`, `depth_min_m`, `depth_max_m`, `tow_type` — those 15 plus a
trailing 16th, `data_stage` — or those 16 plus a trailing 17th,
`source_uuid`. `geom` is minted here as `ST_Point(longitude, latitude)`.
`tow_type` is the net gear code (ichthyo tow/net grains: C1/CB/CV/PV
oblique/vertical, MT manta), NULL for gears/datasets without one. Call
it once per event level — a multi-level dataset (ichthyo
`site`-\>`tow`-\>`net`, bottle `cast`-\>`bottle`) appends one arm per
level, and
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

## Details

`data_stage` is **optional and trailing** on purpose: it records the
source's own processing state for the event (`final` vs `preliminary`
for CTD casts, per question `calcofi_ctd-cast_14`), which most datasets
do not distinguish. Making it positional column 16 rather than inserting
it into the contract lets a dataset opt in when it has a meaningful
stage without touching the other arms — a 15-column arm gets `NULL` and
keeps working unchanged.

`source_uuid` (added 3.32.0, WS-B/Ed Weber's ask) is the provider's own
identifier for *that* event exactly as shipped — ichthyo's `site_uuid` /
`tow_uuid` / `net_uuid` — typed `UUID`, trailing 17th column, same
opt-in-without-disturbing-other-arms reasoning as `data_stage`: a 15- or
16-column arm gets `NULL::UUID` and keeps working unchanged.
