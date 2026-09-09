# Check a `taxa.json` against the release it was built from

One row per check with an `ok` flag, so a release chunk can print the
table whether or not it passes. The counts are re-measured against
`obs_bio` when `con` is given (the release's own connection, or a
promoted release read back); without it the arithmetic that lives inside
the record is still checked.

## Usage

``` r
check_taxa_catalog(record, con = NULL, dataset_record = NULL)
```

## Arguments

- record:

  from
  [`build_taxa_catalog()`](https://calcofi.io/calcofi4db/reference/build_taxa_catalog.md)
  (or a `taxa.json` path or URL)

- con:

  a DBI connection holding `taxon` and `obs_bio`; NULL to check only
  what the record can prove about itself

- dataset_record:

  the `datasets.json` record (or its path) whose `datasets[]` every
  `dataset_key` must appear in; NULL to skip that check

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html): `check`,
`level`, `ok`, `expected`, `observed`, `detail`.

## See also

[`assert_taxa_catalog()`](https://calcofi.io/calcofi4db/reference/assert_taxa_catalog.md),
[`taxa_catalog_checks()`](https://calcofi.io/calcofi4db/reference/taxa_catalog_checks.md)
