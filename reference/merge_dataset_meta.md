# Merge a notebook's structural `dataset_meta` with its descriptive sidecar

The notebook keeps the structural keys
([`dataset_meta_structural_keys()`](https://calcofi.io/calcofi4db/reference/dataset_meta_descriptive_keys.md)),
the sidecar the descriptive ones
([`dataset_meta_descriptive_keys()`](https://calcofi.io/calcofi4db/reference/dataset_meta_descriptive_keys.md));
the merge is their union. A descriptive key found in the notebook with a
value that differs from the sidecar's is an error (two truths); an
identical value is tolerated (and `strict = TRUE`, which
`build_workflows_index.R` uses, errors on any descriptive key left in
the notebook at all). Keys the sidecar carries that are not
`dataset_meta` fields (`provider`, `dataset`, `path`, `status`,
`visibility`, `creators`, …) come along, so a downstream reader can see
them.

## Usage

``` r
merge_dataset_meta(
  notebook_meta,
  sidecar,
  notebook = "notebook",
  sidecar_path = "sidecar",
  strict = FALSE
)
```

## Arguments

- notebook_meta:

  the `calcofi.dataset_meta` list from the notebook YAML

- sidecar:

  the list from
  [`read_dataset_sidecar()`](https://calcofi.io/calcofi4db/reference/read_dataset_sidecar.md)

- notebook, sidecar_path:

  names used in error messages

- strict:

  error on any descriptive key still present in the notebook

## Value

The merged list.
