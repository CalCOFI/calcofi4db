# Create Cruise Key from Ship NODC Code and Date

Creates a natural key for cruises in format YYYY-MM-NODC where:

- YYYY = 4-digit year

- MM = 2-digit month

- NODC = NODC ship code (from ship table's ship_nodc column)

## Usage

``` r
create_cruise_key(
  con,
  cruise_tbl = "cruise",
  ship_tbl = "ship",
  date_col = "date_ym"
)
```

## Arguments

- con:

  DuckDB connection

- cruise_tbl:

  Name of cruise table (default: "cruise")

- ship_tbl:

  Name of ship table (default: "ship")

- date_col:

  Name of date column in cruise table (default: "date_ym")

## Value

Invisibly returns the connection after adding cruise_key column

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_duckdb_con()
create_cruise_key(con)
# produces keys like "1998-02-33JD", "2024-01-33UD"
} # }
```
