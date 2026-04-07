# Collect Ship Mismatches

Finds ships in a table with placeholder `ship_nodc` values (containing
`"?"`) or NULL ship_key, indicating unresolved matches. Used to populate
the `mismatches$ships` section of `manifest.json`.

## Usage

``` r
collect_ship_mismatches(con, table, ship_tbl = "ship")
```

## Arguments

- con:

  DBI connection to DuckDB

- table:

  Table to check for ship mismatches

- ship_tbl:

  Ship reference table (default: `"ship"`)

## Value

Tibble with columns: ship_key, ship_nodc, ship_name, n_rows

## Examples

``` r
if (FALSE) { # \dontrun{
collect_ship_mismatches(con, "ctd_cast")
} # }
```
