# Materialize the WoRMS/ITIS lineage `build_taxon_reference()` reads

Resolves every authority id this dataset's vocabulary reaches — from its
own taxon tables *and* from `measurement_taxon.csv`, which is where the
taxa that had no lineage at all came from — fetches their classification
(cached), and writes it into `con` as the DwC-shaped `taxon` hierarchy
table.

## Usage

``` r
ensure_taxon_lineage(
  con,
  measurement_taxon = NULL,
  overrides = NULL,
  cache_csv = NULL,
  tbl = "taxon",
  refresh = FALSE,
  sleep = 0.3,
  verbose = TRUE,
  xref_cache_csv = .xref_csv_beside(cache_csv)
)
```

## Arguments

- con:

  a DuckDB connection holding this dataset's taxon vocabulary tables

- measurement_taxon:

  the composite crosswalk (`metadata/measurement_taxon.csv`), already
  filtered to this dataset

- overrides:

  the manual id registry (`metadata/taxon_override.csv`)

- cache_csv:

  path to the shared lineage cache (`metadata/taxon_lineage.csv`)

- tbl:

  hierarchy table to write (default `"taxon"` — the name
  [`build_taxon_reference()`](https://calcofi.io/calcofi4db/reference/build_taxon_reference.md)
  reads)

- refresh:

  logical; re-fetch ids already cached

- sleep:

  seconds between API calls (rate limit)

- verbose:

  logical; report what was cached vs fetched

- xref_cache_csv:

  path to the cross-reference cache
  ([`fetch_taxon_xref()`](https://calcofi.io/calcofi4db/reference/fetch_taxon_xref.md)),
  used to top up `_taxon_xref` for the lineage ANCESTORS discovered here
  —
  [`ensure_taxon_xref()`](https://calcofi.io/calcofi4db/reference/ensure_taxon_xref.md)
  runs first and can only see the dataset's own vocabulary. Defaults to
  `taxon_xref.csv` sitting beside `cache_csv`, which is the layout every
  ingest uses; `NULL` skips it.

## Value

(invisibly) a list with `n_ids`, `n_rows` and `n_unresolved`

## Details

Call it **before**
[`build_taxon_reference()`](https://calcofi.io/calcofi4db/reference/build_taxon_reference.md),
which reads that table as its rank / parent / classification authority.
An existing hierarchy is merged, not replaced, so `swfsc_ichthyo` (which
builds its own via
[`build_taxon_hierarchy()`](https://calcofi.io/calcofi4db/reference/build_taxon_hierarchy.md))
keeps what it has and gains only what is missing.
