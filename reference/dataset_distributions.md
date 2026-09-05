# Every endpoint of one dataset, measured and curated

The `distributions[]` of a record (plan § D-1): the parquet objects that
belong to the dataset (its `dataset_key=` partitions and the whole
tables attributed to it), the CF netCDF from its `manifests.json`, the
ERDDAP ids that exist on erddap.calcofi.io now (by `dataset_key` prefix,
with the ISO 19115 record of the primary id), the ingest notebook, the
calcofi.org page, the source portal (`link_data_source`, classified by
host) and the curated rows of `metadata/distribution.csv` for the key —
a legacy ERDDAP id is one of those, listed with `legacy: true` and
whether it is still `live`.

## Usage

``` r
dataset_distributions(
  key,
  ds,
  objects,
  erddap = NULL,
  netcdf = NULL,
  curated = NULL,
  version = NULL,
  workflow_url = NULL
)
```

## Arguments

- key:

  the `dataset_key`

- ds:

  the dataset's `metadata.json` block

- objects:

  the dataset's `objects[]` (from the builder)

- erddap:

  the table from
  [`fetch_erddap_datasets()`](https://calcofi.io/calcofi4db/reference/fetch_erddap_datasets.md),
  or NULL

- netcdf:

  the list from
  [`fetch_netcdf_manifests()`](https://calcofi.io/calcofi4db/reference/fetch_netcdf_manifests.md),
  or NULL

- curated:

  the `distribution.csv` rows for this key (tibble, may be empty)

- version:

  the release version (selects the netCDF entry)

- workflow_url:

  the ingest notebook URL

## Value

A list of rows, each a named list (`kind`, `url`, …).
