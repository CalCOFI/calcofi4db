# Plan a dataset's bundle: which tables, cut how, and what is left out

The tables are the record's own (`datasets.json` `tables[]`) plus the
shared tables a download needs to be used alone — `sample`, `cruise`,
`taxon`, `measurement_type` — wherever the release has them. Each table
is cut to this dataset by one rule:

## Usage

``` r
bundle_plan(
  record,
  table_columns,
  supplemental = character(),
  supplemental_rows = NULL
)
```

## Arguments

- record:

  one dataset record from `datasets.json` (a list with `dataset_key`,
  `tables`)

- table_columns:

  named list, table -\> its column names in this release

- supplemental:

  table names the catalog marks `supplemental`

- supplemental_rows:

  optional named numeric, table -\> rows, quoted in the reason

## Value

A data frame, one row per table in presentation order (`obs`, `sample`,
the dataset's other tables, then the shared vocabularies, then
exclusions): `table`, `included`, `rule`, `link`, `role`, `source`
(`record` \| `companion`), `reason`.

## Details

- `dataset_key` — the table carries a `dataset_key` column: this
  dataset's rows;

- `dataset_key_or_referenced` — `sample` only: this dataset's rows, plus
  the samples its other tables point at (a derived product's
  observations sit on another dataset's casts) and their parents;

- `linked` — a shared table without `dataset_key`: the rows the bundle
  reaches through `link`, the first of `sample_key`, `taxon_key`,
  `measurement_type`, `cruise_key`, `grid_key`, `ship_key` the table
  carries;

- `whole` — a small table with none of those (e.g. `region`, `lookup`);

- `excluded` — a supplemental table (the full-resolution `obs_ctd_full`,
  `obs_mets_full`): hundreds of millions of rows partitioned by cruise,
  left out and named in the README with where to get them;

- `absent` — named by the record but not in this release.

A companion table that ends up with no rows is dropped by
[`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md).

## See also

[`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)
