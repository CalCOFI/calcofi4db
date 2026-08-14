# Apply the vernacular-name registry to a taxon table

Fills `common_name` on `tbl` for taxa that have none of their own. **A
dataset's own vocabulary always wins** — it is the name the provider
publishes, and overwriting it from WoRMS would rename their data under
them. A taxon still awaiting a choice keeps NULL, which publishes no
common name rather than a guessed one.

## Usage

``` r
apply_taxon_common(con, cache_csv, tbl = "taxon", verbose = TRUE)
```

## Arguments

- con:

  DBI connection holding `tbl`.

- cache_csv:

  path to the registry (see
  [`ensure_taxon_common()`](https://calcofi.io/calcofi4db/reference/ensure_taxon_common.md)).

- tbl:

  taxon table name (default `"taxon"`).

- verbose:

  report how many names were filled.

## Value

number of rows filled, invisibly.

## Details

Called by `release_database.qmd` on the merged `taxon` table, so the
registry is applied once rather than in each of the 10 taxa-emitting
ingests.
