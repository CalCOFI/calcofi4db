# Assemble the whole consolidated core from the ingest shards

Convenience wrapper: `sample`, `obs`, `obs_attribute`,
`sample_measurement` (surrogate ids renumbered globally), the
supplemental `obs_ctd_full`, and the taxa references (`taxon` merged
with priority; `dataset_taxon` /`taxon_group` deduplicated). Errors if
`sample_key` is not globally unique — the namespacing guarantee the
whole model rests on.

## Usage

``` r
assemble_core(
  con,
  root = ".",
  supplemental = TRUE,
  parquet_dir = "data/parquet",
  exclude = release_excluded_datasets(root)
)
```

## Arguments

- con:

  a DuckDB connection

- root:

  workflows repo root

- supplemental:

  include `obs_ctd_full` (default TRUE)

- parquet_dir:

  directory holding the per-dataset output dirs

- exclude:

  dataset dir names to skip; defaults to the ingests declaring
  `calcofi.in_release: false` (see
  [`release_excluded_datasets()`](https://calcofi.io/calcofi4db/reference/release_excluded_datasets.md)).
  Resolved once here and threaded to every shard read.

## Value

(invisibly) a named list of row counts
