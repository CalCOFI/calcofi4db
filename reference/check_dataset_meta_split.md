# Assert that no notebook still carries a descriptive `dataset_meta` key

What `scripts/build_workflows_index.R` runs after the sidecar migration:
for every `ingest_*.qmd` whose dataset has a sidecar, the notebook's own
YAML must hold structural keys only.

## Usage

``` r
check_dataset_meta_split(
  workflow_dir,
  pattern = "^ingest_.*\\.qmd$",
  sidecar_dir = NULL
)
```

## Arguments

- workflow_dir:

  Directory containing the `ingest_*.qmd` files.

- pattern:

  Regular expression matching ingest filenames (default
  `"^ingest_.*\\.qmd$"`).

- sidecar_dir:

  the `metadata/` root (default `{workflow_dir}/metadata`)

## Value

Invisibly, a tibble
`notebook, dataset_key, has_sidecar, descriptive_in_notebook`.
