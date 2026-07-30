# Datasets held out of the release by `calcofi.in_release: false`

An ingest under development can be run end-to-end — writing its own
`data/parquet/{provider}_{dataset}/` outputs, manifest and sidecars —
while being kept out of the frozen release, by adding
`in_release: false` to its `calcofi:` YAML block. This returns those
datasets' labels, which is what every release-side discovery step
filters on: the table registry
([`build_release_table_registry()`](https://calcofi.io/calcofi4db/reference/build_release_table_registry.md)),
the core shard union
([`core_shard_paths()`](https://calcofi.io/calcofi4db/reference/core_shard_paths.md)),
and `release_database.qmd`'s `relationships.json` / `metadata.json` /
`manifest.json` globs.

## Usage

``` r
release_excluded_datasets(workflow_dir, pattern = "^ingest_.*\\.qmd$")
```

## Arguments

- workflow_dir:

  Directory containing the `ingest_*.qmd` files.

- pattern:

  Regular expression matching ingest filenames.

## Value

Character vector of `provider_dataset` labels (the `data/parquet/`
subdirectory names) to exclude; empty when every ingest is in the
release.

## Details

Omitting the key means the ingest IS in the release, so existing
notebooks are unaffected.

## Examples

``` r
if (FALSE) { # \dontrun{
release_excluded_datasets("workflows")   # "dfw_dungeness-crab"
} # }
```
