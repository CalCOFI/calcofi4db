# Fetch (and cache) the WoRMS \<-\> ITIS cross-reference for a set of taxa

Resolves each requested identifier through the authority that can answer
it, and returns one row per query with the **accepted** ids, the
authority's real `status`, and the date it was checked:

## Usage

``` r
fetch_taxon_xref(
  itis_ids = integer(),
  worms_ids = integer(),
  names = character(),
  cache_csv = NULL,
  refresh = FALSE,
  sleep = 0.3,
  verbose = TRUE
)
```

## Arguments

- itis_ids:

  integer ITIS TSNs to crosswalk (NA/duplicates dropped)

- worms_ids:

  integer WoRMS AphiaIDs to backfill an `itis_id` for

- names:

  character source names; cleaned with
  [`clean_taxon_name()`](https://calcofi.io/calcofi4db/reference/clean_taxon_name.md)
  first

- cache_csv:

  path to the cross-reference cache CSV (`metadata/taxon_xref.csv`);
  read if it exists, rewritten when anything new is fetched. `NULL`
  fetches everything and caches nothing.

- refresh:

  logical; re-fetch queries already cached (and re-date them)

- sleep:

  seconds between API calls (rate limit)

- verbose:

  logical; report what was cached vs fetched

## Value

a data.frame of cross-reference rows for the requested queries

## Details

- `itis_ids` — exact TSN -\> AphiaID crosswalk via
  `worrms::wm_record_by_external(type = "tsn")`, plus the ITIS-accepted
  TSN via
  [`taxize::itis_acceptname()`](https://docs.ropensci.org/taxize/reference/itis_acceptname.html).
  This is where a bird gains its `worms_id` without losing its `itis:`
  key.

- `worms_ids` — the reverse direction,
  `worrms::wm_external(type = "tsn")`, backfilling `itis_id` on
  WoRMS-keyed taxa.

- `names` —
  [`worrms::wm_records_name()`](https://docs.ropensci.org/worrms/reference/wm_records_name.html)
  on
  [`clean_taxon_name()`](https://calcofi.io/calcofi4db/reference/clean_taxon_name.md)
  output, the fallback for taxa carrying neither id.

Queries already present in `cache_csv` are not re-fetched, so a re-run
is free and offline. `notes` accumulates datestamped lines and is never
rewritten.
