# Derive Cruise Key on Bottle Casts via Ship Matching

Full pipeline to link bottle `casts` to the SWFSC `cruise` table by: (1)
finding unmatched ship codes, (2) running
[`match_ships()`](https://calcofi.io/calcofi4db/reference/match_ships.md)
for fuzzy matching, (3) adding `ship_key` and `cruise_key` columns to
casts, (4) validating against the cruise table.

## Usage

``` r
derive_cruise_key_on_casts(con, ship_renames_csv = NULL, fetch_ices = TRUE)
```

## Arguments

- con:

  DBI connection to DuckDB with casts, ship, and cruise tables

- ship_renames_csv:

  Optional path to manual ship overrides CSV

- fetch_ices:

  Logical; if TRUE, also query ICES ship API (default TRUE)

## Value

List with components:

- `ship_matches`: tibble of ship match results

- `cruise_stats`: tibble of cruise bridge match statistics

- `unmatched_report`: tibble of unmatched ship codes

## Details

The cruise_key format is YYYY-MM-NODC (4-digit year, 2-digit month, NODC
ship code), e.g. "1998-02-33JD".

Requires that `ship` and `cruise` tables are already loaded in the
DuckDB connection (e.g., via
[`load_prior_tables()`](https://calcofi.io/calcofi4db/reference/load_prior_tables.md)).

## Examples

``` r
if (FALSE) { # \dontrun{
# after loading casts, ship, cruise tables
result <- derive_cruise_key_on_casts(
  con              = con,
  ship_renames_csv = here("metadata/calcofi/bottle/ship_renames.csv"))
result$cruise_stats
} # }
```
