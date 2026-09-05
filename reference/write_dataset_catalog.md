# Write `datasets.json` and one `datasets/{key}.json` per dataset

Write `datasets.json` and one `datasets/{key}.json` per dataset

## Usage

``` r
write_dataset_catalog(record, dir)
```

## Arguments

- record:

  from
  [`build_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/build_dataset_catalog.md)

- dir:

  the release directory

## Value

The paths written, invisibly (`datasets.json` first).
