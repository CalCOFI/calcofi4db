# What changed since the last observation

A change is a proposal, never a silent edit (plan § D-11): a new EDI
revision, a dead link, a DOI newly minted or an ERDDAP `date_modified`
that moved is one row here, for the dataset's `questions.csv` and the
provider's Sheet.

## Usage

``` r
distribution_changes(observed, previous = NULL)
```

## Arguments

- observed:

  the tibble from
  [`observe_distributions()`](https://calcofi.io/calcofi4db/reference/observe_distributions.md)

- previous:

  the tibble from
  [`read_distribution_observed()`](https://calcofi.io/calcofi4db/reference/read_distribution_observed.md),
  or NULL

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html)
`dataset_key`, `url`, `field`, `was`, `now` — empty when nothing moved
(or when there is no previous file).
