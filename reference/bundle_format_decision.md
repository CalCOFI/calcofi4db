# Which formats a dataset's bundle is offered in, and why one is not

- `csv` — always. `excel_openable` only when every table is within
  Excel's sheet limit
  ([EXCEL_MAX_ROWS](https://calcofi.io/calcofi4db/reference/EXCEL_MAX_ROWS.md),
  header included); otherwise `note` says which table is too long to
  open in a spreadsheet.

- `xlsx` — only when every table has at most `xlsx_max_rows` rows of
  data
  ([BUNDLE_XLSX_MAX_ROWS](https://calcofi.io/calcofi4db/reference/EXCEL_MAX_ROWS.md),
  so every sheet fits Excel's 1,048,576 rows with its header); otherwise
  not written, and `reason` says why in plain words.

- `parquet` — always.

## Usage

``` r
bundle_format_decision(rows, xlsx_max_rows = BUNDLE_XLSX_MAX_ROWS)
```

## Arguments

- rows:

  named numeric, table -\> rows in the bundle

- xlsx_max_rows:

  the largest table an Excel workbook is offered for

## Value

A data frame, one row per
[`bundle_formats()`](https://calcofi.io/calcofi4db/reference/bundle_formats.md):
`format`, `write`, `excel_openable`, `reason` (why a format is not
offered, `""` otherwise) and `note` (a caveat on a format that is
offered, `""` otherwise).
