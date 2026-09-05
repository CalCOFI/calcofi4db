# Read the calcofi YAML block from a single workflow file

Read the calcofi YAML block from a single workflow file

## Usage

``` r
read_calcofi_meta(qmd_path, sidecar_dir = NULL)
```

## Arguments

- qmd_path:

  Path to one `ingest_*.qmd` (or any .qmd with a `calcofi:` YAML block).

- sidecar_dir:

  The `metadata/` root holding `{provider}/{dataset}/dataset_meta.yml`,
  the descriptive half of `dataset_meta` (default: `metadata/` beside
  the notebook). When the sidecar exists it is merged in by
  [`merge_dataset_meta()`](https://calcofi.io/calcofi4db/reference/merge_dataset_meta.md)
  and its path is recorded as `dataset_meta_sidecar`.

## Value

The parsed `calcofi` block as a list, augmented with `provider_dataset`
and `qmd`, or `NULL` if absent. Use this in an ingest's setup chunk to
read its own `provider`/`dataset`/ `tables_owned` from the authoritative
YAML rather than hard-coding.
