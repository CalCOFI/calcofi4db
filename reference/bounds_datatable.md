# Render a bounds check as the standard notebook table

The `#### Values outside their declared range` section an ingest
notebook shows. Colours `status` so `out_of_range` and `undeclared` are
legible at a glance, and drops the `finding` column — it is long prose
meant for a `questions.csv` `context` cell, not for a table.

## Usage

``` r
bounds_datatable(
  x,
  caption = paste("Measured values against the registry's declared bounds.",
    "`undeclared` is a finding too: nothing was checked."),
  page_length = 25
)
```

## Arguments

- x:

  a tibble from
  [`check_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/check_measurement_bounds.md)

- caption:

  table caption

- page_length:

  rows per page

## Value

A [`DT::datatable()`](https://rdrr.io/pkg/DT/man/datatable.html)
htmlwidget.

## Examples

``` r
if (FALSE) { # \dontrun{
bounds_datatable(check_measurement_bounds(con, "obs"))
} # }
```
