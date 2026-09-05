# Read `metadata/distribution.csv`, the curated endpoints per dataset

One row per external endpoint a dataset can be got or seen through that
the release cannot measure itself — a CoastWatch mirror, the EDI or NCEI
record the ingest read from, the OBIS dataset and its IPT resource, a
legacy ERDDAP id with its sunset — with `kind`, `portal`, `id`, `url`,
`title`, `status`
(`current | superseded | retired | external | planned`), `superseded_by`
(a URL or a `dataset_key`), `observed_utc` (when the status was last
confirmed;
[`observe_distributions()`](https://calcofi.io/calcofi4db/reference/observe_distributions.md)
will refresh it) and `notes`. Nothing is ever deleted from it: a dead
endpoint becomes `retired` with the date, so a page can say "was at X
until …". Every vocabulary column is validated; an unknown value errors.

## Usage

``` r
read_distribution_registry(path)
```

## Arguments

- path:

  path to `metadata/distribution.csv`

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html), all
columns character.
