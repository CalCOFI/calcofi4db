# Append-or-update `metadata/measurement_scale.csv` by (key, label)

`kind` is one of `familiar | physical | threshold | computed` (D7). A
`computed` mark's `value` is recomputed at build time from `how` (the
function and its inputs) and is never trusted from the CSV — this
registry holds the recipe and a value for reference, not the release's
number.

## Usage

``` r
register_measurement_scale(new_rows, path, quiet = FALSE)
```

## Arguments

- new_rows:

  data.frame with `key`, `label` and any other column of
  `.measurement_scale_cols()`

- path:

  path to `metadata/measurement_scale.csv`

- quiet:

  suppress the added/updated message

## Value

the full updated registry
