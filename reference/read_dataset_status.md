# Read `metadata/dataset_status.csv`, the pipeline-stage tracker

One row per (provider, dataset) with the stage, priority, GitHub issue,
blockers and the `publish_*` registration columns (`publish_obis`,
`publish_erddap`, `publish_edi`, `publish_ncei`, `publish_caloos`),
whose cells read `done`, `n/a`, `planned` or `#38;#39 planned` — see
[`parse_registration()`](https://calcofi.io/calcofi4db/reference/parse_registration.md).

## Usage

``` r
read_dataset_status(path)
```

## Arguments

- path:

  path to `metadata/dataset_status.csv`

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html) with
`dataset_key` added, all columns character.
