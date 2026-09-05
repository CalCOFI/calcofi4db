# Build an EML document for every dataset in the catalog record

Build an EML document for every dataset in the catalog record

## Usage

``` r
build_eml_catalog(
  catalog,
  sidecars = NULL,
  meta = NULL,
  coverage = NULL,
  gear = NULL
)
```

## Arguments

- catalog:

  the record from
  [`build_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/build_dataset_catalog.md)
  (or a `datasets.json` path)

- sidecars:

  the named list from
  [`read_dataset_sidecars()`](https://calcofi.io/calcofi4db/reference/read_dataset_sidecars.md)
  (or a `registries` list, whose `sidecars` element is used)

- meta, coverage, gear:

  passed to
  [`build_eml()`](https://calcofi.io/calcofi4db/reference/build_eml.md)

## Value

A named list of EML documents, keyed by `dataset_key`.
