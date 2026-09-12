# Per-dataset row signatures of a release's objects

For each object of each table in `tables` (read off `catalog.json`'s
`objects[]`), one row per dataset holding rows in it:

## Usage

``` r
publish_object_signatures(
  con,
  catalog,
  tables,
  owners = NULL,
  cache = NULL,
  base_url = "https://storage.googleapis.com/calcofi-db"
)
```

## Arguments

- con:

  a DuckDB connection with `httpfs` loaded when `base_url` is remote

- catalog:

  the parsed (`simplifyVector = FALSE`) `catalog.json`

- tables:

  table names to sign

- owners:

  optional named list, table -\> the dataset_keys that read it (from
  `datasets.json`'s `tables[]`); a table read by exactly one dataset is
  never scanned

- cache:

  optional CSV path of the signature cache (created if absent)

- base_url:

  prefix joined to each object's `path` (`/`-separated)

## Value

A data frame: `table`, `content_hash`, `dataset_key` (`"*"` for a whole
object), `signature`. A table absent from the catalog gives one row with
`content_hash = "<missing>"`.

## Details

- an object hive-partitioned by `dataset_key` — its own `content_hash`,
  no read;

- an object of a table only one dataset reads (`owners`), or with no
  `dataset_key` column at all (a vocabulary such as `measurement_type`)
  — `dataset_key = "*"` and its `content_hash`: the whole object is an
  input of every dataset that reads the table;

- any other object — read once, `GROUP BY dataset_key`, each group's row
  signature (the one
  [`freeze_plan()`](https://calcofi.io/calcofi4db/reference/freeze_plan.md)
  uses to decide `upload`/`copy`), cached in `cache` under the object's
  `content_hash` so an unchanged object is never read again.

## See also

[`publish_data_parts()`](https://calcofi.io/calcofi4db/reference/publish_data_parts.md),
[`publish_fingerprint()`](https://calcofi.io/calcofi4db/reference/publish_fingerprint.md)
