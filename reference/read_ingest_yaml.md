# Read calcofi YAML blocks from ingest\_\*.qmd front matter

Reads the `calcofi:` block from the YAML front matter of each
`ingest_*.qmd` workflow and returns it keyed by
`"\{provider\}_\{dataset\}"`. This is the authoritative source for
dataset-level metadata, table ownership, workflow links, and ERD colors
consumed by
[`merge_metadata_json()`](https://calcofi.io/calcofi4db/reference/merge_metadata_json.md)
and `release_database.qmd`.

## Usage

``` r
read_ingest_yaml(
  workflow_dir,
  pattern = "^ingest_.*\\.qmd$",
  in_release_only = FALSE
)
```

## Arguments

- workflow_dir:

  Directory containing the `ingest_*.qmd` files.

- pattern:

  Regular expression matching ingest filenames (default
  `"^ingest_.*\\.qmd$"`).

- in_release_only:

  Drop ingests that declare `calcofi.in_release: false` (default
  `FALSE`, i.e. return every ingest). `release_database.qmd` passes
  `TRUE` so an in-progress ingest does not reach the release's `dataset`
  table, ERD or `metadata.json`. See
  [`release_excluded_datasets()`](https://calcofi.io/calcofi4db/reference/release_excluded_datasets.md).

## Value

A named list keyed by `provider_dataset`. Each element is the parsed
`calcofi` block, augmented with `provider_dataset` and `qmd` (the source
file path). Ingests whose block lacks `provider`/`dataset` are skipped
with a warning.

## Examples

``` r
if (FALSE) { # \dontrun{
ingest_yaml <- read_ingest_yaml("workflows")
names(ingest_yaml)            # "swfsc_ichthyo" "calcofi_bottle" ...
ingest_yaml$calcofi_bottle$tables_owned
} # }
```
