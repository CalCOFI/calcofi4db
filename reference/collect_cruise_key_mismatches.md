# Collect Cruise Key Mismatches

Finds cruise keys that are malformed (do not match `YYYY-MM-NODC`
pattern) or contain placeholder `"?"` characters from interim ship
entries. Used to populate the `mismatches$cruise_keys` section of
`manifest.json`.

## Usage

``` r
collect_cruise_key_mismatches(con, table)
```

## Arguments

- con:

  DBI connection to DuckDB

- table:

  Table to check for cruise_key mismatches

## Value

Tibble with columns: cruise_key, status, n_rows

## Examples

``` r
if (FALSE) { # \dontrun{
collect_cruise_key_mismatches(con, "ctd_cast")
} # }
```
