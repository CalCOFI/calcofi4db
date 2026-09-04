# Read `metadata/license.csv`, the registry of dataset licenses

One row per SPDX-style id an ingest's `dataset_meta.license` may carry
(`CC-BY-4.0`, `CC0-1.0`, `CC-BY-NC-4.0`, `CC-BY-SA-4.0`, `US-PD`,
`custom`, `unknown`, …) with `name`, `url`, `status` (`active` \|
`deprecated`) and `notes`. Read strictly (`na = ""`) and validated like
every other registry: sentinel strings, an unknown status or a duplicate
id are errors. `custom` requires a `license_url` on the dataset;
`unknown` (or an empty license) fails the index unless a `questions.csv`
row is open on it — see
[`check_dataset_citation()`](https://calcofi.io/calcofi4db/reference/check_dataset_citation.md).

## Usage

``` r
read_license_registry(path, validate = TRUE)
```

## Arguments

- path:

  path to `metadata/license.csv`

- validate:

  error on a malformed registry (default TRUE)

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html), all
columns character.
