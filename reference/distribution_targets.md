# Every external endpoint worth observing: the registry plus the holdings' links

A holding (`status: planned | external | archived` in its descriptive
sidecar) has no release objects, so its only endpoints are its
`link_data_source` and its DOI — plan § D-11 asks for them to be
observed exactly like an ingested dataset's. Rows are deduplicated on
`(dataset_key, url)`, the registry's row winning (it carries the curated
`id`, `title` and `status`).

## Usage

``` r
distribution_targets(registry, sidecars = NULL)
```

## Arguments

- registry:

  the tibble from
  [`read_distribution_registry()`](https://calcofi.io/calcofi4db/reference/read_distribution_registry.md)

- sidecars:

  the named list from
  [`read_dataset_sidecars()`](https://calcofi.io/calcofi4db/reference/read_dataset_sidecars.md),
  or NULL

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html) with the
registry's columns.
