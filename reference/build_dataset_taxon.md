# Build the `dataset_taxon` crosswalk (per-dataset vocabulary -\> `taxon`)

One row per (dataset, local taxon): the dataset's own `ds_taxon_key`
(`"<dataset-or-known-list>:<local id>"`, all lowercase — e.g.
`calcofi:19` for the shared CalCOFI species list, `cce-lter_zoodb:3`
otherwise), its `ds_scientific_name` / `ds_common_name` /
`ds_taxa_code`, and the global `taxon_key` it resolves to. Deduped on
`ds_taxon_key`.

## Usage

``` r
build_dataset_taxon(
  con,
  measurement_taxon = NULL,
  overrides = NULL,
  tbl = "dataset_taxon"
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

  target table name (default `"dataset_taxon"`)

## Value

(invisibly) the row count written
