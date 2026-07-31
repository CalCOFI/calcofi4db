# Supplemental full-resolution tables declared by the ingests

Reads every ingest's `calcofi.tables_owned` and returns the tables
flagged `supplemental: true` — the full-resolution products that ship
alongside the thinned core (`obs_ctd_full`, `obs_mets_full`), hosted and
tagged to the release but hidden from the default table list and the
ERD.

## Usage

``` r
supplemental_core_tables(root = ".", which = TRUE)
```

## Arguments

- root:

  workflows repo root.

- which:

  `TRUE` for all declared, `FALSE` for none, or an explicit character
  vector (returned as given, so a caller can override).

## Value

Character vector of table names, possibly empty.

## Details

Only `obs`-shaped tables (named `obs_*`) are returned. A supplemental
table that is not a slice of the core cannot be assembled by
[`assemble_core()`](https://calcofi.io/calcofi4db/reference/assemble_core.md),
which renumbers `obs_id` and orders by the core's columns —
`calcofi_mets` previously declared the raw `mets_measurement` here,
which carried neither an `obs_id` nor any coordinate and could not be
published usefully.

## Examples

``` r
if (FALSE) { # \dontrun{
supplemental_core_tables("workflows")   # "obs_ctd_full" "obs_mets_full"
} # }
```
