# Create Interactive Data Table with CSV Export

Wrapper around DT::datatable() with sensible defaults for workflow
output tables: CSV download button, top filters, horizontal scroll, and
adaptive page lengths.

## Usage

``` r
dt(d, fname, n_page = 10, ...)
```

## Arguments

- d:

  Data frame to display

- fname:

  Filename for CSV export (without extension)

- n_page:

  Number of rows per page (default: 10)

- ...:

  Additional arguments passed to DT::datatable()

## Value

A DT::datatable widget
