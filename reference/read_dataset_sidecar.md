# Read one descriptive sidecar, `metadata/{provider}/{dataset}/dataset_meta.yml`

The provider-editable half of a dataset's metadata (plan § D-9):
abstract, methods, creators, contact, keywords, licence, citation, DOI,
links — and, for a **holding** (a dataset with no release yet, § D-11),
the structural keys too plus `status: planned | external | archived`,
`priority`, `owner`, `next_step`, `gh_issue`, `module`. `visibility`
defaults to `public`. Validates the vocabularies; a licence outside
`license.csv` is refused here when the registry is given.

## Usage

``` r
read_dataset_sidecar(path, licenses = NULL)
```

## Arguments

- path:

  the YAML file

- licenses:

  optional character vector of active licence ids

## Value

A named list; `NULL` when the file does not exist.
