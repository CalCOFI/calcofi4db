# The `holdings.csv` index, generated from the holding sidecars

One row per sidecar with `status: planned | external | archived` (plan §
D-11):
`key, name, category, provider, status, link, doi, module, lead_name, lead_email, lead_affiliation, priority_caloos, gh_issue, notes`.
The lead columns come from the first `creators[]` entry; `link` is
`link_data_source`. `write_holdings_csv()` writes it with `na = ""`.

## Usage

``` r
holdings_from_sidecars(registries)

write_holdings_csv(registries, path)
```

## Arguments

- registries:

  from
  [`read_catalog_registries()`](https://calcofi.io/calcofi4db/reference/read_catalog_registries.md)
  (only `sidecars` is read)

- path:

  where to write `holdings.csv`

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html).
