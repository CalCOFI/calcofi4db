# Build a dataset's bundle tables in a DuckDB connection

For each table
[`bundle_plan()`](https://calcofi.io/calcofi4db/reference/bundle_plan.md)
includes, one table `{prefix}{table}` in `con`: the dataset's rows cut
by the plan's rule, geometry and H3 cell columns dropped (latitude and
longitude stay), and readable columns added beside keys so a row reads
without a join:

## Usage

``` r
bundle_build_tables(con, plan, dataset_key, from_sql, prefix = "bundle_")
```

## Arguments

- con:

  a DuckDB connection

- plan:

  the data frame from
  [`bundle_plan()`](https://calcofi.io/calcofi4db/reference/bundle_plan.md)

- dataset_key:

  the dataset

- from_sql:

  named character, table -\> a `FROM` expression for the release table
  (`read_parquet('…')`, or a parenthesised view such as the catalog's
  `obs`)

- prefix:

  name prefix of the tables written

## Value

A data frame, one row per table written (presentation order): `table`,
`rows`, `n_columns`, `dropped` (comma-separated columns left out),
`added` (comma-separated columns added). A companion table with no rows
is dropped and not listed.

## Details

- beside `sample_key`: `site_key`, `line`, `station` (from `sample`;
  `line` and `station` parsed from a `site_key` of the form
  `090.0 060.0`, empty otherwise);

- beside `site_key` on `sample` itself: `line`, `station`;

- beside `taxon_key`: `scientific_name`, `common_name` (from `taxon`);

- beside `measurement_type`: `units` (from `measurement_type`).

A column is only added to a table that does not already carry one by
that name, and never to the table it comes from. Rows are stored under
`ORDER BY ALL`, so a table's order is a function of its content (the
zips are hashed). Intermediate tables are dropped.

## See also

[`bundle_plan()`](https://calcofi.io/calcofi4db/reference/bundle_plan.md),
[`write_bundle()`](https://calcofi.io/calcofi4db/reference/write_bundle.md)
