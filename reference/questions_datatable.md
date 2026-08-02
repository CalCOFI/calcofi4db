# Render a question registry as the standard notebook table

The `## Questions for Data Providers` section every ingest notebook ends
with. One call so the 16 notebooks cannot show different columns in
different orders — which they did, each with its own hand-written
priority factor.

## Usage

``` r
questions_datatable(
  x,
  caption = "Questions for data providers (ranked)",
  page_length = 25
)
```

## Arguments

- x:

  a path to a `questions.csv`, or a data.frame from
  [`read_questions()`](https://calcofi.io/calcofi4db/reference/read_questions.md)

- caption:

  table caption

- page_length:

  rows per page

## Value

A [`DT::datatable()`](https://rdrr.io/pkg/DT/man/datatable.html)
htmlwidget.

## Details

Columns that are empty for every question are dropped, so a dataset with
no answers yet does not render two blank columns.

## Examples

``` r
if (FALSE) { # \dontrun{
questions_datatable(here::here(cc$questions_file))
} # }
```
