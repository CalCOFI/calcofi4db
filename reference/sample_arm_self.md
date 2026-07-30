# Build a `sample` arm for a single self-contained event table

The declarative shape most datasets need: one `sample` row per row of
one event table, keyed `dataset_key:sample_type:id`, with no parent.
Exported because a dataset's projection belongs in its own ingest
notebook — this is what keeps that a one-line declaration rather than
copied SQL.

## Usage

``` r
sample_arm_self(
  dataset_key,
  tbl,
  id_col,
  sample_type,
  dt_col = "datetime_start_utc",
  grid_expr = "grid_key",
  site_expr = "NULL::VARCHAR",
  ord_expr = "NULL::INTEGER",
  depth_min = "0::DOUBLE",
  depth_max = "0::DOUBLE"
)
```

## Arguments

- dataset_key:

  provider_dataset

- tbl:

  the event table

- id_col:

  its id column

- sample_type:

  core `sample_type` value

- dt_col:

  datetime column, or `"NULL"` for none

- grid_expr, site_expr, ord_expr, depth_min, depth_max:

  SQL expressions or bare column names (bare names are alias-qualified
  for you)

## Value

a SQL SELECT string for
[`append_sample()`](https://calcofi.io/calcofi4db/reference/append_sample.md)
