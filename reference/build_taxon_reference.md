# Build the unified `taxon` reference table

Assembles one authoritative row per distinct `taxon_key` across every
dataset's local taxa **plus the WoRMS lineage ancestors** (from the
pre-built `taxon` hierarchy table, so `parent_taxon_key` chains resolve
for descendant expansion). Duplicate taxa across datasets collapse —
e.g. Appendicularia (AphiaID 146421) in both `zoodb_taxon` and
`zooscan_taxon` becomes one `worms:146421` row. Names/rank/lineage are
coalesced with source priority (WoRMS hierarchy \> CalCOFI species /
seabird-mammal \> per-dataset lineage \> composite crosswalk).
`rank_order` folds in the old `taxa_rank` lookup.

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

  a DuckDB connection with the per-dataset taxon tables loaded

- measurement_taxon:

  optional data.frame of the composite-type crosswalk
  (`metadata/measurement_taxon.csv`) so cufes/phyllosoma/euphausiid
  taxa, which live in `measurement_type` names not a taxon table, are
  included

- overrides:

  optional data.frame of manual id resolution
  (`metadata/taxon_override.csv`) for coarse taxa (phyto groups,
  mammals)

- tbl:

  target table name (default `"taxon"`)

## Value

(invisibly) the row count written
