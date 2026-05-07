# Report Ship Matching Status for a Dataset

Summarizes ship matching results for a given dataset table: how many
rows have matched ship_key, how many are unmatched, and lists any new
ships not yet in the reference table.

## Usage

``` r
report_ship_matches(con, dataset_label, table = NULL, ship_tbl = "ship")
```

## Arguments

- con:

  DBI connection to DuckDB

- dataset_label:

  Character label for the dataset (e.g., "ichthyo", "bottle")

- table:

  Name of table to check (default: auto-detect from dataset_label)

- ship_tbl:

  Name of ship reference table (default: "ship")

## Value

Tibble with columns: ship_key, ship_nodc, ship_name, n_rows, status

## Examples

``` r
if (FALSE) { # \dontrun{
report_ship_matches(con, "bottle")
report_ship_matches(con, "ichthyo", table = "cruise")
} # }
```
