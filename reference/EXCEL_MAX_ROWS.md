# Limits a bundle is held to

- `EXCEL_MAX_ROWS` — Excel's own sheet limit, 1,048,576 rows including
  the header.

- `BUNDLE_XLSX_MAX_ROWS` — the largest table an Excel workbook is
  offered for: Excel's own limit, 1,048,575 rows of data below the
  header row (owner's decision, 2026-11-16: any lower cap would be
  arbitrary). Above it the CSV zip is the route.

- `BUNDLE_MAX_ZIP_BYTES` — the largest zip
  [`write_bundle()`](https://calcofi.io/calcofi4db/reference/write_bundle.md)
  will leave for a browser download, 2 GiB; over it the build stops and
  says so rather than dropping the file.

## Usage

``` r
EXCEL_MAX_ROWS

BUNDLE_XLSX_MAX_ROWS

BUNDLE_MAX_ZIP_BYTES
```

## Format

Integers / a number.
