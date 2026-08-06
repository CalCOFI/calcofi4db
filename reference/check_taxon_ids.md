# The taxa that no authority resolved, per dataset — reported, and gated

A taxon that reaches the release without an authority id is invisible to
any consumer that filters or joins on one, and nothing used to say so.
That is how all 128 Farallon taxa and 64,956 observations became
unreachable through `db-viz-hex::get_sp()`'s `worms_id` join while every
check in the pipeline passed.

## Usage

``` r
check_taxon_ids(con, allow = character(), halt = TRUE, verbose = TRUE)
```

## Arguments

- con:

  a DBI connection holding `taxon` and `dataset_taxon` (and `obs`, if
  present, for the observation-level counts)

- allow:

  character vector of dataset-local `taxon_key`s that are known to be
  unresolvable and are accepted as such

- halt:

  logical; [`stop()`](https://rdrr.io/r/base/stop.html) on an
  unallowlisted local key (default `TRUE`)

- verbose:

  logical; message the summary

## Value

a data.frame, one row per `dataset_key`, with the taxon- and
observation-level counts (invisibly when `verbose = FALSE`)

## Details

Two conditions, deliberately graded differently:

- **A dataset-local `taxon_key`** (no `worms:` / `itis:` prefix) means
  *no authority resolved this taxon at all*. This **fails** unless the
  key is in `allow` — the allowlist is where a genuinely non-taxonomic
  class such as `cce-lter_zooscan:16` (naupliar stage) is declared, in
  the open, one key at a time. A new unresolved taxon can then never
  hide among the known ones.

- **An authority key with no `worms_id`** is reported but does not fail:
  WoRMS legitimately lacks some taxa (trinomial subspecies, mostly), and
  an `itis:`-keyed bird is correctly keyed either way.
