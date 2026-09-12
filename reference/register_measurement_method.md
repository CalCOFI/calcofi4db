# Append-or-update `metadata/measurement_method.csv` by (dataset_key, measurement_type)

One row per series in `measurements.json` (D4): `platform` in
`bottle | ctd | underway | lab | net | mast`; `nerc_l22` only on an
exact device match (empty otherwise, per the `metadata-registries`
skill's exact-match rule); `steps` and `bibkeys` are `" | "`-joined
lists on one CSV cell. A method the source cannot support ships with
`principle` empty and `source = "not found"` — never a guess.

## Usage

``` r
register_measurement_method(new_rows, path, quiet = FALSE)
```

## Arguments

- new_rows:

  data.frame with `dataset_key`, `measurement_type` and any other column
  of `.measurement_method_cols()`

- path:

  path to `metadata/measurement_method.csv`

- quiet:

  suppress the added/updated message

## Value

the full updated registry
