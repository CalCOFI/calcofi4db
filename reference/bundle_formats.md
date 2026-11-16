# The download formats a dataset bundle can be written in

`csv` (every dataset), `xlsx` (only when every table fits; see
[`bundle_format_decision()`](https://calcofi.io/calcofi4db/reference/bundle_format_decision.md))
and `parquet` (every dataset), in the order a page offers them.

## Usage

``` r
bundle_formats()
```

## Value

Character vector.
