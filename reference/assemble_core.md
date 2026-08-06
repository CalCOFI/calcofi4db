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
  parquet_dir = cc_stage_path("parquet"),
  exclude = release_excluded_datasets(root)
)
```

## Arguments

- con:

  a DuckDB connection

- root:

  workflows repo root

- supplemental:

  `TRUE` (default) to include every supplemental full-resolution table
  the ingests declare, `FALSE` for none, or an explicit character vector
  of table names. See
  [`supplemental_core_tables()`](https://calcofi.io/calcofi4db/reference/supplemental_core_tables.md).

- parquet_dir:

  directory holding the per-dataset output dirs. Defaults to the local
  staging root (see
  [`cc_stage_dir()`](https://calcofi.io/calcofi4db/reference/cc_stage_dir.md)),
  where the bulk parquet lives; an absolute path is used as-is, a
  relative one is resolved against `root`. The JSON sidecars stay in the
  repo and are found separately.

- exclude:

  dataset dir names to skip; defaults to the ingests declaring
  `calcofi.in_release: false` (see
  [`release_excluded_datasets()`](https://calcofi.io/calcofi4db/reference/release_excluded_datasets.md)).
  Resolved once here and threaded to every shard read.

## Value

(invisibly) a named list of row counts
