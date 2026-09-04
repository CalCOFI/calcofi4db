# Build the unified `taxon` reference table

Assembles one authoritative row per distinct `taxon_key` across every
dataset's local taxa **plus the WoRMS/ITIS lineage ancestors** (from the
pre-built `taxon` hierarchy table, so `parent_taxon_key` chains resolve
for descendant expansion). Duplicate taxa across datasets collapse —
e.g. Appendicularia (AphiaID 146421) in both `zoodb_taxon` and
`zooscan_taxon` becomes one `worms:146421` row. Names/rank/lineage are
coalesced by **source kind**, not by dataset: the flattened
classification (the authority) first, then the hierarchy, then the
vocabularies in `dataset_key` order. There is no list of datasets to
maintain. `rank_order` folds in the old `taxa_rank` lookup.

## Usage

``` r
build_taxon_reference(
  con,
  measurement_taxon = NULL,
  overrides = NULL,
  tbl = "taxon"
)
```

## Arguments

- con:

  a DuckDB connection with the staged vocabulary loaded

- measurement_taxon:

  optional data.frame of the composite-type crosswalk
  (`metadata/measurement_taxon.csv`) so cufes/phyllosoma/crab taxa,
  which live in `measurement_type` names not a taxon table, are included

- overrides:

  optional data.frame of manual id resolution
  (`metadata/taxon_override.csv`) for coarse taxa (phyto groups,
  mammals)

- tbl:

  target table name (default `"taxon"`)

## Value

(invisibly) the row count written

## Details

`common_name` in this shard is the dataset's own; the release applies
the written precedence centrally with
[`apply_taxon_common()`](https://calcofi.io/calcofi4db/reference/apply_taxon_common.md).
