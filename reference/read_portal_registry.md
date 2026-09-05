# Read `metadata/portal.csv`, the portal capability registry

The table `docs/portals.qmd` used to hand-maintain
(`data/portal_comparison.csv`), now a registry with the two columns the
catalog needs: `harvests_from_us` (what the portal reads from calcofi.io
— `erddap-waf`, `sitemap-jsonld`, `data.json`, `ipt`, `none`) and
`observe_method` (how
[`observe_distributions()`](https://calcofi.io/calcofi4db/reference/observe_distributions.md)
asks it what it holds now — `edi-pasta`, `doi`, `obis-api`,
`ncbi-esummary`, `zenodo-api`, `erddap-das`, `caloos`, `http`).

## Usage

``` r
read_portal_registry(path)
```

## Arguments

- path:

  path to `metadata/portal.csv`

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html), all
columns character.
