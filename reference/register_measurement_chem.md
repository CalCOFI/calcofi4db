# Append-or-update `metadata/measurement_chem.csv` by (key, chebi_id, role)

`role` is one of
`the_thing | component | conjugate | method_product | one_of` and `via`
one of `nerc_s27 | registry` (Appendix A). Refuses a row whose `role` or
`via` is outside that vocabulary, or whose `source` is empty — a
measurement-face row without a source is the one thing this registry
never ships (WS-MF1 gate).

## Usage

``` r
register_measurement_chem(new_rows, path, quiet = FALSE)
```

## Arguments

- new_rows:

  data.frame with `key`, `chebi_id`, `role` and any of `mass_fraction`,
  `via`, `source`, `source_url`, `note`

- path:

  path to `metadata/measurement_chem.csv`

- quiet:

  suppress the added/updated message

## Value

the full updated registry
