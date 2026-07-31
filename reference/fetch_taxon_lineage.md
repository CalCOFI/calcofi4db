# Fetch (and cache) the WoRMS/ITIS lineage for a set of taxon ids

One row per (requested taxon, ancestor-or-self), which is what both
halves of a usable hierarchy need: the ancestor rows so
`parent_taxon_key` chains resolve, and the ranks so
`kingdom`/`phylum`/`class`/`order_taxon`/`family` can be flattened onto
each taxon.

## Usage

``` r
fetch_taxon_lineage(
  worms_ids = integer(),
  itis_ids = integer(),
  cache_csv = NULL,
  refresh = FALSE,
  sleep = 0.3,
  verbose = TRUE
)
```

## Arguments

- worms_ids:

  integer vector of AphiaIDs to resolve (NA/duplicates dropped)

- itis_ids:

  integer vector of ITIS TSNs to resolve (the Aves-keyed taxa)

- cache_csv:

  path to the lineage cache CSV; read if it exists, rewritten when
  anything new is fetched. `NULL` fetches everything and caches nothing.

- refresh:

  logical; re-fetch ids already cached

- sleep:

  seconds between API calls (rate limit)

- verbose:

  logical; report what was cached vs fetched

## Value

a data.frame of lineage rows for the requested ids

## Details

Ids already present in `cache_csv` are not re-fetched, so a re-run is
free and offline. The cache is a reviewable registry like the others
under `metadata/`; it is written with `na = ""` (see the round-trip trap
in the workflows `CLAUDE.md` — `readr`'s default turns an empty cell
into the two-character string `"NA"`, which DuckDB then reads as data).
