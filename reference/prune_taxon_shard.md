# Prune the taxa references to one dataset's shard

[`build_taxon_reference()`](https://calcofi.io/calcofi4db/reference/build_taxon_reference.md)
/
[`build_dataset_taxon()`](https://calcofi.io/calcofi4db/reference/build_dataset_taxon.md)
/
[`build_taxon_group()`](https://calcofi.io/calcofi4db/reference/build_taxon_group.md)
read *whichever* taxon sources are present in `con`, which inside an
ingest is normally just that dataset's vocabulary — but not always. An
ingest may have loaded another dataset's tables as references, and
`swfsc_ichthyo` holds a WoRMS **lineage hierarchy**
([`build_taxon_hierarchy()`](https://calcofi.io/calcofi4db/reference/build_taxon_hierarchy.md))
that is broader than the taxa its own observations reach. This trims all
three to this dataset's shard so the release union stays small and the
shards stay disjoint-ish.

## Usage

``` r
prune_taxon_shard(con, dataset_key)
```

## Arguments

- con:

  a DuckDB connection holding `taxon` / `dataset_taxon` (and optionally
  `taxon_group`) as built by the three builders

- dataset_key:

  provider_dataset to keep

## Value

(invisibly) a named list of the surviving row counts

## Details

Lineage **ancestors are kept**: descendant expansion walks
`parent_taxon_key`, so dropping an ancestor would break the chain. The
kept set is therefore the transitive parent closure of
`dataset_taxon.taxon_key`, not just the directly-referenced taxa.

Generic — there is nothing dataset-specific here beyond the
`dataset_key` argument. Call it after the three builders in an ingest
that either holds a hierarchy table or has other datasets' vocabulary
tables in scope.
