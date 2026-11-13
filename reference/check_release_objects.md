# Check a release catalog's objects as published

Three checks per table of a `catalog.json`, supplemental tables included
(4.38.0):

## Usage

``` r
check_release_objects(
  catalog,
  con = NULL,
  base_url = "https://storage.googleapis.com/calcofi-db",
  tables = NULL
)
```

## Arguments

- catalog:

  a parsed `catalog.json` (list, as
  `jsonlite::fromJSON(..., simplifyVector = FALSE)` returns it, or a
  data-frame form).

- con:

  optional DuckDB connection able to read `base_url` (httpfs loaded for
  https); without it `published_rows` is not checked.

- base_url:

  where the catalog's bucket-relative `path`s resolve.

- tables:

  optional subset of table names.

## Value

A tibble: `table`, `check`, `ok`, `n` (objects, or published rows),
`expected`, `detail`.

## Details

- `unique_paths`: no object path is listed twice;

- `one_object_per_partition`: a partitioned table has one object per
  partition value (and at most one single-file twin, an object without a
  partition value holding every row, as `obs` publishes), a single-file
  table exactly one object;

- `published_rows` (only with `con`): the rows in the parquet footers of
  the table's distinct partition objects, read where they are published
  (no scan), sum to the catalog's `rows`, and so do a twin's.

v2026.11.05 and v2026.11.09 failed all three for `obs_ctd_full`: 169
objects for 143 cruises, 24 paths listed 2 or 3 times, and 240,092,665
published rows against a catalog `rows` of 287,281,956, because the
export wrote some partitions as two or three files and the uploads
overwrote each other (see
[`export_release_parquet()`](https://calcofi.io/calcofi4db/reference/export_release_parquet.md)).
