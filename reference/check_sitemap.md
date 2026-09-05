# Check a generated sitemap

The structural half always runs (https, uniqueness, `lastmod` shape, the
pages leading); the network half asks every URL with a one-byte ranged
GET, like the index's link check, and is skipped when
`CALCOFI_SKIP_LINK_CHECK` is set.

## Usage

``` r
check_sitemap(
  d,
  network = !nzchar(Sys.getenv("CALCOFI_SKIP_LINK_CHECK")),
  probe = NULL
)
```

## Arguments

- d:

  the tibble from
  [`build_datasets_sitemap()`](https://calcofi.io/calcofi4db/reference/build_datasets_sitemap.md)

- network:

  probe every URL (default: off when `CALCOFI_SKIP_LINK_CHECK`)

- probe:

  a function `url -> integer status` (the tests inject one)

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html) `loc`,
`finding`, `level`, `detail`.
