# Write `metadata/distribution_observed.json`

Write `metadata/distribution_observed.json`

## Usage

``` r
write_distribution_observed(observed, path, changes = NULL)
```

## Arguments

- observed:

  the tibble from
  [`observe_distributions()`](https://calcofi.io/calcofi4db/reference/observe_distributions.md)

- path:

  the file to write

- changes:

  the tibble from
  [`distribution_changes()`](https://calcofi.io/calcofi4db/reference/distribution_changes.md),
  or NULL

## Value

`path`, invisibly.
