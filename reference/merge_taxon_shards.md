# Merge the per-dataset `taxon` shards into one authoritative reference

Each ingest emits the taxon rows its own vocabulary reaches. The same
taxon can appear in several shards (Appendicularia is in both zoodb and
zooscan), so rows are collapsed on `taxon_key` and each field takes the
first non-NULL value in **dataset directory order**. There is no
priority list (taxon plan D5): every shard's `scientific_name` / `rank`
/ classification comes from the same cached authority lineage, so shards
agree wherever both have a value and the order only settles which shard
fills a gap. `common_name` is not decided here at all — the release
applies the written precedence with
[`apply_taxon_common()`](https://calcofi.io/calcofi4db/reference/apply_taxon_common.md)
— and `notes` is unioned, never picked.

## Usage

``` r
merge_taxon_shards(
  con,
  root = ".",
  parquet_dir = cc_stage_path("parquet"),
  exclude = release_excluded_datasets(root)
)
```

## Arguments

- con:

  a DuckDB connection

- root:

  workflows repo root

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
