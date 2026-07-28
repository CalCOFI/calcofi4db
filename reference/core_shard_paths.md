# Find the per-dataset parquet shards for a core table

Find the per-dataset parquet shards for a core table

## Usage

``` r
core_shard_paths(table, root = ".", parquet_dir = "data/parquet")
```

## Arguments

- table:

  core table name (e.g. `"obs"`)

- root:

  workflows repo root (contains `data/parquet/`)

- parquet_dir:

  directory holding the per-dataset output dirs

## Value

character vector of readable parquet paths/globs, one per dataset
