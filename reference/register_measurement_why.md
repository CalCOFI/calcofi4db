# Append-or-update `metadata/measurement_why.csv` by (key, rank)

`rank` 1 is the pick shown on the page; ranks 2+ are alternatives under
a collapsed details element (D5). Owned by WS-MF2 — WS-MF1 creates the
file with its header only.

## Usage

``` r
register_measurement_why(new_rows, path, quiet = FALSE)
```

## Arguments

- new_rows:

  data.frame with `key`, `rank` and any other column of
  `.measurement_why_cols()`

- path:

  path to `metadata/measurement_why.csv`

- quiet:

  suppress the added/updated message

## Value

the full updated registry
