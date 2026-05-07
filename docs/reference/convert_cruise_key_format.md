# Convert Old YYMMKK Cruise Key to YYYY-MM-NODC Format

Converts legacy cruise key format (YYMMKK: 2-digit year + 2-digit
month + 2-letter ship_key) to the new format (YYYY-MM-NODC: 4-digit
year + 2-digit month + NODC ship code). Requires the ship table for KK →
NODC mapping.

## Usage

``` r
convert_cruise_key_format(
  con,
  table,
  old_key_col = "cruise_key",
  ship_tbl = "ship"
)
```

## Arguments

- con:

  DuckDB connection

- table:

  Name of table containing old cruise keys

- old_key_col:

  Name of column with old YYMMKK keys (default: "cruise_key")

- ship_tbl:

  Name of ship table (default: "ship")

## Value

Invisibly returns the connection after updating cruise_key column

## Details

Century disambiguation: YY \>= 49 → 19YY, YY \< 49 → 20YY (CalCOFI data
spans 1949-present).

## Examples

``` r
if (FALSE) { # \dontrun{
# convert CTD cruise keys extracted from filenames
convert_cruise_key_format(con, "ctd_cast", "cruise_key_old")
} # }
```
