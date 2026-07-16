# Build the `taxon_group` grouping table (many taxa per group)

Seeds portable, cross-dataset groupings (`taxon_group_key` =
`"<dataset-or-known-list>:<group>"`, lowercase; a `description`; many
`taxon_key`) from the groupings each dataset already carries —
phytoplankton functional groups (`phyto_taxon.taxa`, e.g.
`diatom, centric`) and the seabird/mammal `is_bird`/`is_mammal` flags.
Curated `calcofi:*` groups (e.g. `forage_fish`) can be appended later.

## Usage

``` r
build_taxon_group(
  con,
  measurement_taxon = NULL,
  overrides = NULL,
  tbl = "taxon_group"
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

  target table name (default `"taxon_group"`)

## Value

(invisibly) the row count written
