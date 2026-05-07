# Ensure Interim Ship Entries for Unmatched Ships

For any ship codes that remain unmatched after
[`match_ships()`](https://calcofi.io/calcofi4db/reference/match_ships.md),
inserts interim placeholder rows into the `ship` table with
`ship_nodc = "?SK?"` (where `SK` is the 2-letter ship_key). This allows
downstream operations (cruise_key derivation, FK joins) to proceed
without errors. Placeholder ships are flagged via the `"?"` markers for
later resolution via `metadata/ship_renames.csv`.

## Usage

``` r
ensure_interim_ships(con, match_result, ship_tbl = "ship")
```

## Arguments

- con:

  DBI connection to DuckDB with a `ship` table

- match_result:

  Tibble from
  [`match_ships()`](https://calcofi.io/calcofi4db/reference/match_ships.md)
  with `match_type` and `ship_code` columns. Rows with
  `match_type == "unmatched"` get interim entries.

- ship_tbl:

  Name of ship table (default: `"ship"`)

## Value

Integer count of interim ships inserted

## Examples

``` r
if (FALSE) { # \dontrun{
result <- match_ships(unmatched, reference)
n_interim <- ensure_interim_ships(con, result)
} # }
```
