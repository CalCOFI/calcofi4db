# Build the dataset registry table from ingest YAML blocks

Assembles a data frame mirroring the legacy `metadata/dataset.csv` (the
authoritative `dataset` registry table written into each ingest's
database) from the `dataset_meta`/`tables_owned` YAML blocks returned by
[`read_ingest_yaml()`](https://calcofi.io/calcofi4db/reference/read_ingest_yaml.md).
Replaces reading `dataset.csv`.

## Usage

``` r
ingest_yaml_to_dataset_df(ingest_yaml)
```

## Arguments

- ingest_yaml:

  Named list from
  [`read_ingest_yaml()`](https://calcofi.io/calcofi4db/reference/read_ingest_yaml.md).

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html) with one
row per dataset (including any `additional_datasets` folded into an
ingest) and the columns of the legacy `dataset.csv`.
