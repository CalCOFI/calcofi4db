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
  release_date = as.character(Sys.Date())
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

## Value

A list ready for `jsonlite::write_json(auto_unbox = TRUE)`.
