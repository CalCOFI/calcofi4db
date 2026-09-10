# The `flags[]` a `measurements.json` measurement (a page) can carry

One flag, and it is about the record rather than the data: `no_label`
says no `metadata/variable.csv` row supplies a display label for the
key, so `label` fell back to the canonical series' registry
`description` — a column note, not a title. The builder never invents a
label.

## Usage

``` r
measurement_flags()
```

## Value

A character vector, the measurement-level `flags` enum.

## Examples

``` r
measurement_flags()
#> [1] "no_label"
```
