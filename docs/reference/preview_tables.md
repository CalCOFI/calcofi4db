# Preview Tables with Head and Tail Rows

For each table, emits a markdown header and shows the first and last `n`
rows as DT datatables. Small tables (≤ 2n rows) are shown in full.
Geometry columns are excluded from display since they render as
unreadable binary.

## Usage

``` r
preview_tables(con, tables, n = 100, table_header_level = 3)
```

## Arguments

- con:

  DBI connection to DuckDB

- tables:

  Character vector of table names to preview.

- n:

  Integer. Number of rows for head and tail (default: 100).

- table_header_level:

  Integer. Markdown heading level for each table (default: 3, i.e.
  `###`).

## Value

Invisible NULL. Side effect: prints markdown headers and DT datatable
widgets for Quarto/knitr output.

## Details

Use in a chunk with `#| results: asis` so the markdown headers render
correctly.

## Examples

``` r
if (FALSE) { # \dontrun{
# in a chunk with `#| results: asis`
preview_tables(con, c("casts", "bottle", "grid"))
} # }
```
