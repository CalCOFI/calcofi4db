# Standardize Site Key from Line and Station Columns

Creates a `site_key` column formatted as `NNN.N NNN.N` (0-padded, 1
decimal) from line and station columns. This ensures consistent site
identification across all datasets.

## Usage

``` r
standardize_site_key(
  con,
  table,
  line_col = "line",
  station_col = "station",
  site_key_col = "site_key"
)
```

## Arguments

- con:

  DuckDB connection

- table:

  Name of table to update

- line_col:

  Name of line column (default: "line")

- station_col:

  Name of station column (default: "station")

- site_key_col:

  Name of site key column to create (default: "site_key")

## Value

Invisibly returns the connection after adding site_key column

## Examples

``` r
if (FALSE) { # \dontrun{
standardize_site_key(con, "site", "line", "station")
# line=90.0, station=62.0 → "090.0 062.0"
# line=93.3, station=110.0 → "093.3 110.0"
} # }
```
