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
  priority = c("swfsc_ichthyo", "calcofi_bird_mammal_census", "cce-lter_zoodb",
    "cce-lter_zooscan", "calcofi_phytoplankton"),
  parquet_dir = "data/parquet"
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

  directory holding the per-dataset output dirs

## Value

(invisibly) the row count of the merged `taxon`
