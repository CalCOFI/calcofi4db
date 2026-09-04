# Build the release catalog with per-table hashes and objects

Keeps every field consumers read today (`name`, `rows`, `partitioned`,
`supplemental`) and adds `content_hash`, `bytes`, `objects[]` (`path`,
`bytes`, `sha256`, `content_hash`, `since`, `partition_by`,
`partition_value`) and, for the canonical layout, `compat_path`.

## Usage

``` r
build_release_catalog(
  version,
  tables_df,
  plan,
  layout = "compat",
  release_date = as.character(Sys.Date()),
  views = release_views()
)
```

## Arguments

- version:

  release version.

- tables_df:

  data.frame with `name, rows, partitioned, supplemental`.

- plan:

  tibble from
  [`freeze_plan()`](https://calcofi.io/calcofi4db/reference/freeze_plan.md).

- layout:

  as in
  [`freeze_plan()`](https://calcofi.io/calcofi4db/reference/freeze_plan.md).

- release_date:

  character date.

- views:

  the view registry, see
  [`release_views()`](https://calcofi.io/calcofi4db/reference/release_views.md);
  [`list()`](https://rdrr.io/r/base/list.html) for none.

## Value

A list ready for `jsonlite::write_json(auto_unbox = TRUE)`.

## Details

Since 3.31.0 the catalog also carries a top-level **`views`** map — view
name → SQL over `{{table}}` tokens — for every entry of `views` whose
source tables are all in `tables_df`, and the table a view `replaces`
gains `deprecated: true`, `replaced_by: [...]` and `removed_in` while it
still ships. A resolver (`calcofi4r::cc_get_db()`,
`calcofi4py.cc_get_db()`, db-query) creates the views after the tables;
a deprecated table's objects are read only when the view's sources were
not loaded.
