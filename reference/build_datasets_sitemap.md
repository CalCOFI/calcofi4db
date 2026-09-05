# Build the rows of `datasets/sitemap.xml` from the record

Build the rows of `datasets/sitemap.xml` from the record

## Usage

``` r
build_datasets_sitemap(
  record,
  observed = NULL,
  edited = NULL,
  include_holdings = TRUE,
  changefreq = "weekly"
)
```

## Arguments

- record:

  `datasets.json`: a path/URL or the parsed list

- observed:

  the tibble from
  [`read_distribution_observed()`](https://calcofi.io/calcofi4db/reference/read_distribution_observed.md),
  or NULL — its `updated` / `observed_utc` date is an external record's
  `lastmod`, and a row observed `retired` is dropped even when the
  registry still calls it current

- edited:

  a named character vector `dataset_key -> ISO date` of the descriptive
  sidecars' last edit; a page's `lastmod` is the later of that and the
  release date

- include_holdings:

  list the holdings' pages too (default TRUE — a holding has a page at
  the same URL shape, plan § D-11)

- changefreq:

  the `changefreq` written for every URL

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html): `loc`,
`lastmod`, `changefreq`, `kind` (`page` + `portal = "calcofi.io"` for a
dataset page, else the distribution's own kind and portal),
`dataset_key`, `portal`, `title`. Pages first, in record order, then the
external records; every `loc` unique.
