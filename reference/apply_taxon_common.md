# Apply the common-name precedence to the merged `taxon` table

Sets `common_name` on `tbl` for every taxon as one `COALESCE`, in this
order (taxon plan D5):

## Usage

``` r
apply_taxon_common(
  con,
  cache_csv,
  tbl = "taxon",
  dataset_taxon = "dataset_taxon",
  curated = "swfsc_ichthyo",
  group_rules = NULL,
  verbose = TRUE
)
```

## Arguments

- con:

  DBI connection holding `tbl` (and `dataset_taxon`, for ranks 2 and 4;
  without it those ranks are simply empty).

- cache_csv:

  path to the registry (see
  [`ensure_taxon_common()`](https://calcofi.io/calcofi4db/reference/ensure_taxon_common.md)).

- tbl:

  taxon table name (default `"taxon"`).

- dataset_taxon:

  crosswalk table name (default `"dataset_taxon"`).

- curated:

  the dataset whose vocabulary is rank 2 (default `"swfsc_ichthyo"`, the
  CalCOFI species list).

- group_rules:

  the group registry as read by
  [`read_taxon_group_rules()`](https://calcofi.io/calcofi4db/reference/read_taxon_group_rules.md),
  or a path to it; its `dataset_taxon` rules' `match_value`s are labels
  rank 4 refuses. `NULL` (the default) refuses only the labels of
  dataset-local keys, which need no registry to recognise.

- verbose:

  report how many names each rank supplied.

## Value

a data.frame of per-rank counts (`rank`, `source`, `n`) — the five ranks
plus `other_excluded_label`, the taxa whose only rank-4 candidate was a
refused label — invisibly.

## Details

1.  a **human choice** in the registry (`source = "manual"`) — the
    override;

2.  the **curated species list's** own name —
    `dataset_taxon.ds_common_name` where `dataset_key = curated`
    (`swfsc_ichthyo`, CalCOFI's own names);

3.  **WoRMS**, when it offers exactly one English vernacular
    (`source = "worms"`, `n_candidates_en = 1`);

4.  any **other dataset's** `ds_common_name`, in `dataset_key` order
    (this is where the seabird and marine-mammal names come from — WoRMS
    holds almost no bird vernaculars) — **except a label that is not a
    name**: a value that is a `match_value` of a `dataset_taxon` rule in
    `group_rules` (`metadata/taxon_group.csv`: "diatom, centric",
    "other", …) or the `ds_common_name` of any dataset-local
    (non-authority) key ("undefined (code not in source definitions;
    Q05)", zooscan "nauplii"). Those are what a source calls a *group*
    or an *operational class*, and rank 4 used to publish them as the
    common name of every taxon in the group (Ben, 2026-09-04: a group
    label is never a `common_name`; the group's own name in
    `taxon_group` is unchanged). The taxa that lose their only rank-4
    candidate this way are counted as `other_excluded_label`;

5.  empty. Never a guess.

The merged table's existing `common_name` is **not** a rank: it is
whichever shard won the merge, which is the undocumented order this
replaces.

When two codes of one dataset resolve to the same taxon (ichthyo 683
*Sebastes* "Rockfishes" and 3023 *Sebastes crocotulus* "Sunset rockfish"
both carry the genus AphiaID), the code whose `ds_scientific_name`
equals `taxon.scientific_name` — the code that *is* the taxon rather
than one finer or coarser than it — wins; failing that, `ds_taxon_key`
ascending.

Called by `release_database.qmd` on the merged `taxon` table, so the
registry is applied once rather than in each of the 10 taxa-emitting
ingests.
