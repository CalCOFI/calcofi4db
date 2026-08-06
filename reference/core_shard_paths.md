# Find the per-dataset parquet shards for a core table

Find the per-dataset parquet shards for a core table

## Usage

``` r
core_shard_paths(
  table,
  root = ".",
  parquet_dir = cc_stage_path("parquet"),
  exclude = release_excluded_datasets(root)
)
```

## Arguments

- table:

  core table name (e.g. `"obs"`)

- root:

  workflows repo root (contains `data/parquet/`)

- parquet_dir:

  directory holding the per-dataset output dirs. Defaults to the local
  staging root (see
  [`cc_stage_dir()`](https://calcofi.io/calcofi4db/reference/cc_stage_dir.md)),
  where the bulk parquet lives; an absolute path is used as-is, a
  relative one is resolved against `root`. The JSON sidecars stay in the
  repo and are found separately.

- exclude:

  dataset dir names to skip; defaults to the ingests that declare
  `calcofi.in_release: false` (see
  [`release_excluded_datasets()`](https://calcofi.io/calcofi4db/reference/release_excluded_datasets.md)),
  so an in-progress ingest's shards stay out of the release even though
  its parquet is on disk

## Value

character vector of readable parquet paths/globs, one per dataset
