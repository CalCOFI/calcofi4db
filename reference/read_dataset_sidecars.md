# Read every descriptive sidecar under a `metadata/` root

Read every descriptive sidecar under a `metadata/` root

## Usage

``` r
read_dataset_sidecars(metadata_dir, licenses = NULL)
```

## Arguments

- metadata_dir:

  the `workflows/metadata` directory

- licenses:

  passed to
  [`read_dataset_sidecar()`](https://calcofi.io/calcofi4db/reference/read_dataset_sidecar.md)

## Value

A named list keyed by `dataset_key` (`{provider}_{dataset}` from the
directory path); each element carries `provider`, `dataset` and `path`.
