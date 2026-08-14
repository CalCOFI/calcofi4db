# Fetch and cache vernacular (common) names from WoRMS

**This is where a multi-vernacular choice is made, and a human makes
it.** WoRMS returns English vernaculars as an unordered bag with no
preferred-name flag, so:

## Usage

``` r
ensure_taxon_common(
  taxa,
  cache_csv = NULL,
  refresh = FALSE,
  sleep = 0.3,
  verbose = TRUE
)
```

## Arguments

- taxa:

  data frame with `taxon_key`, `worms_id` and `scientific_name` (extra
  columns ignored). Rows with no `worms_id` are skipped — there is
  nothing to ask WoRMS about.

- cache_csv:

  path to the registry. Required: this is pointless without a place to
  record the choice.

- refresh:

  re-query taxa already cached (hand-picked names still survive).

- sleep:

  seconds between WoRMS calls.

- verbose:

  report progress and how many await a choice.

## Value

the registry, invisibly.

## Details

- **exactly one** English name — taken automatically, since there is no
  choice;

- **two or more** — `common_name` is left empty, every candidate is
  written to `candidates_en`, and someone picks by editing the cell.
  Nothing is guessed, and an unresolved taxon simply publishes no common
  name;

- **none** — recorded with `n_candidates_en = 0` so it is not
  re-queried.

A re-run never overwrites a non-empty `common_name`, so a hand-picked
value is permanent even under `refresh = TRUE`.

## Examples

``` r
if (FALSE) { # \dontrun{
ensure_taxon_common(taxa, cache_csv = here("metadata/taxon_common.csv"))
} # }
```
