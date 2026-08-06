# Merge the per-dataset `taxon` shards into one authoritative reference

Each ingest emits the taxon rows its own vocabulary reaches. The same
taxon can appear in several shards (Appendicularia is in both zoodb and
zooscan), so rows are collapsed on `taxon_key` and each field takes the
first non-NULL value by source priority — the WoRMS-lineage-bearing
shard (`swfsc_ichthyo`, which carries the hierarchy) first, then the
curated seabird/mammal and plankton vocabularies, then everything else.
This reproduces the coalescing
[`build_taxon_reference()`](https://calcofi.io/calcofi4db/reference/build_taxon_reference.md)
did when it saw every source at once.

## Usage

``` r
merge_taxon_shards(
  con,
  root = ".",
  priority = c("swfsc_ichthyo", "farallon_bird-mammal", "cce-lter_zoodb",
    "cce-lter_zooscan", "calcofi_phytoplankton"),
  parquet_dir = cc_stage_path("parquet"),
  exclude = release_excluded_datasets(root)
)
```

## Arguments

- con:

  a DuckDB connection

- root:

  workflows repo root

- priority:

  dataset dirs in descending priority

- parquet_dir:

  directory holding the per-dataset output dirs. Defaults to the local
  staging root (see
  [`cc_stage_dir()`](https://calcofi.io/calcofi4db/reference/cc_stage_dir.md)),
  where the bulk parquet lives; an absolute path is used as-is, a
  relative one is resolved against `root`. The JSON sidecars stay in the
  repo and are found separately.

- exclude:

  dataset dir names to skip (see
  [`core_shard_paths()`](https://calcofi.io/calcofi4db/reference/core_shard_paths.md))

## Value

(invisibly) the row count of the merged `taxon`
