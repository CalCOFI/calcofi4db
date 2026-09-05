# The `dataset_meta` keys that live in the descriptive sidecar, not the notebook

Plan § D-9 (Decision 14) splits an ingest's `calcofi.dataset_meta` in
two: the **structural** keys stay in the notebook YAML (`dataset_name`,
`dataset_name_short`, `category`, `color`, `tables`, `in_release` — what
the pipeline needs to run and group), and the **descriptive** keys — the
ones a provider edits in a Google Sheet — move to
`metadata/{provider}/{dataset}/dataset_meta.yml`.
[`read_calcofi_meta()`](https://calcofi.io/calcofi4db/reference/read_calcofi_meta.md)
merges the two; `scripts/build_workflows_index.R` errors when a
descriptive key is still found in a notebook.

## Usage

``` r
dataset_meta_descriptive_keys()

dataset_meta_structural_keys()
```

## Value

Character vector of key names.
