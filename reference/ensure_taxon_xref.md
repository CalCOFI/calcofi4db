# Materialize the authority cross-reference `.taxon_norm_sources()` reads

Works out which identifiers this dataset's vocabulary reaches, resolves
them (cached), and stages the result in `con` as `_taxon_xref`. Every
taxon builder then picks it up automatically: `worms_id` is filled on
ITIS-keyed taxa, `itis_id` on WoRMS-keyed ones, deprecated ids are
replaced by their accepted form, and `taxonomic_status` /
`status_checked` / `notes` are carried through.

## Usage

``` r
ensure_taxon_xref(
  con,
  measurement_taxon = NULL,
  overrides = NULL,
  cache_csv = NULL,
  tbl = "_taxon_xref",
  refresh = FALSE,
  sleep = 0.3,
  verbose = TRUE
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

  path to the shared cross-reference cache (`metadata/taxon_xref.csv`)

- tbl:

  staging table to write (default `"_taxon_xref"`)

- refresh:

  logical; re-fetch queries already cached (and re-date them)

- sleep:

  seconds between API calls (rate limit)

- verbose:

  logical; report what was cached vs fetched

## Value

(invisibly) a list with `n_queries`, `n_resolved`, `n_rekeyed`

## Details

Call it **before**
[`ensure_taxon_lineage()`](https://calcofi.io/calcofi4db/reference/ensure_taxon_lineage.md)
and the three builders — the lineage fetch should ask about the accepted
id, not the deprecated one.
