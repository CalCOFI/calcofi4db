# The ingest asserts its own taxon crosswalk (taxon plan D6)

Call it after
[`resolve_dataset_taxon()`](https://calcofi.io/calcofi4db/reference/resolve_dataset_taxon.md)
and before
[`append_obs()`](https://calcofi.io/calcofi4db/reference/append_obs.md).
Three findings, each a row of the returned report:

## Usage

``` r
check_dataset_taxon(
  con,
  dataset_key,
  allow = character(),
  halt = TRUE,
  codes = NULL,
  verbose = TRUE
)
```

## Arguments

- con:

  a DBI connection holding `dataset_taxon` (and `taxon`)

- dataset_key:

  the dataset whose crosswalk is checked

- allow:

  character vector of `taxon_key`s accepted as-is: dataset-local keys of
  non-taxonomic classes, or a `worms:` key for an Aves taxon with no TSN

- halt:

  logical; [`stop()`](https://rdrr.io/r/base/stop.html) on any finding
  (default `TRUE`)

- codes:

  optional character vector of the `ds_taxa_code`s the observations
  reference (e.g. `DISTINCT species_code` of the source observation
  table); every one must be in the vocabulary

- verbose:

  logical; message the summary

## Value

a data.frame with one row per finding (`check`, `ds_taxon_key`,
`ds_taxa_code`, `taxon_key`, `detail`); zero rows when clean. Invisible
when `verbose = FALSE`.

## Details

- **`missing_code`** — a code the observations reference (`codes`) that
  is not in this dataset's `dataset_taxon`. Farallon's `MEGU` (the
  pre-split Mew Gull code, present in the observations and absent from
  the species list) is the case that motivated it: an `obs` projection
  joining on the code would drop or NULL those rows with no error
  anywhere.

- **`unresolved`** — a `dataset_taxon` row with no authority `taxon_key`
  (`worms:` / `itis:`), unless its dataset-local key is in `allow` — the
  ingest's own declaration of a genuinely non-taxonomic class (zooscan
  "nauplii", phyto "undefined code"), one key at a time, with a comment.

- **`aves_not_itis`** — a taxon whose class is Aves that did not key
  `itis:` (no accepted TSN resolved; see
  [`taxon_key_of()`](https://calcofi.io/calcofi4db/reference/taxon_key_of.md)).
  Checked here because this is where it is cheap to fix — a TSN in
  `taxon_override.csv`. An ingest that accepts the `worms:` key lists
  that key in `allow`. Needs `taxon` in `con`
  ([`build_taxon_reference()`](https://calcofi.io/calcofi4db/reference/build_taxon_reference.md))
  for the class; skipped without it.

`release_database.qmd`'s
[`check_taxon_ids()`](https://calcofi.io/calcofi4db/reference/check_taxon_ids.md)
stays as the backstop.
