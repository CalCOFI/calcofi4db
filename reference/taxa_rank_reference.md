# The canonical taxonomic rank ordering (`taxa_rank`)

One row per rank, ordered kingdom-down, so `taxon.rank_order` sorts a
hierarchy without a consumer hard-coding rank names.

## Usage

``` r
taxa_rank_reference()
```

## Value

a data.frame of `taxonRank` + `rank_order`

## Details

This used to be a vector inside
[`build_taxon_hierarchy()`](https://calcofi.io/calcofi4db/reference/build_taxon_hierarchy.md),
which exactly one ingest calls — so the `taxa_rank` lookup existed in
the `swfsc_ichthyo` connection and nowhere else, and
[`build_taxon_reference()`](https://calcofi.io/calcofi4db/reference/build_taxon_reference.md)'s
left join to it produced `rank_order = NA` for every other dataset's
taxa. In release v2026.08.06 that was **100% of ITIS-keyed taxa** (all
169, i.e. every seabird and marine mammal) plus 252 WoRMS-keyed ones —
172 species, 83 genera and 49 families with no sortable rank.

The vocabulary spans BOTH authorities. WoRMS and ITIS do not use the
same rank set, and eight ranks the release actually carries were absent
from the old vector — `Gigaclass`, `Infrakingdom`, `Megaclass`,
`Parvphylum`, `Phylum (Division)`, `Subphylum (Subdivision)`,
`Subterclass`, `Superdomain` — so those taxa had no `rank_order` even
where the lookup was present.

Ordering is by nesting depth, not by a strict Linnaean canon: what a
consumer needs is "does this rank sit above or below that one", and ties
are harmless.

## Examples

``` r
head(taxa_rank_reference())
#>      taxonRank rank_order
#> 1  Superdomain          1
#> 2       Domain          2
#> 3       Empire          3
#> 4      Kingdom          4
#> 5   Subkingdom          5
#> 6 Infrakingdom          6
```
