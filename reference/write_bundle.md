# Write a dataset's bundle zips

For each format
[`bundle_format_decision()`](https://calcofi.io/calcofi4db/reference/bundle_format_decision.md)
offers, a folder named like the zip
([`bundle_file_name()`](https://calcofi.io/calcofi4db/reference/bundle_file_name.md)
without `.zip`) holding the tables and `README.md`, `CITATION.md`,
`CITATION.bib`, `LICENSE.txt` and `columns.csv`, zipped with
[`bundle_zip()`](https://calcofi.io/calcofi4db/reference/bundle_zip.md)
(fixed order and timestamps, so unchanged content gives the same sha256)
and then removed. CSV and Parquet come straight from the tables in `con`
(`COPY … TO`); the Parquet uses the release's pinned writer settings on
one thread; the Excel workbook is written with `writexl` (a `README`
sheet first, a `columns` sheet last) and its internal timestamp fixed.

## Usage

``` r
write_bundle(
  con,
  record,
  release,
  tables,
  plan,
  out_dir,
  meta_columns = list(),
  meta_tables = list(),
  xlsx_max_rows = BUNDLE_XLSX_MAX_ROWS,
  max_bytes = BUNDLE_MAX_ZIP_BYTES,
  prefix = "bundle_",
  licenses_dir = system.file("licenses", package = "calcofi4db")
)
```

## Arguments

- con:

  the connection
  [`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)
  wrote into

- record:

  one dataset record from `datasets.json`

- release:

  a list: `version`, `release_date`, `citation`, `doi`, `concept_doi`

- tables:

  the data frame from
  [`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)

- plan:

  the data frame from
  [`bundle_plan()`](https://calcofi.io/calcofi4db/reference/bundle_plan.md)

- out_dir:

  where the zips are written (created)

- meta_columns, meta_tables:

  `metadata.json`'s `columns` and `tables` blocks

- xlsx_max_rows:

  passed to
  [`bundle_format_decision()`](https://calcofi.io/calcofi4db/reference/bundle_format_decision.md)

- max_bytes:

  the largest zip allowed

- prefix:

  the prefix
  [`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)
  used

- licenses_dir:

  passed to
  [`bundle_license()`](https://calcofi.io/calcofi4db/reference/bundle_license.md)

## Value

A data frame, one row per
[`bundle_formats()`](https://calcofi.io/calcofi4db/reference/bundle_formats.md):
`format`, `written`, `file`, `path`, `bytes`, `sha256`,
`excel_openable`, `reason`, `note`.

## Details

A zip over `max_bytes` stops the build with its size: a file too large
for a browser download is a decision for a person, never dropped
silently.

## See also

[`bundle_manifest_entry()`](https://calcofi.io/calcofi4db/reference/bundle_manifest_entry.md)
