# Replace UUIDs with Integer Foreign Keys

Creates an integer ID column and populates it based on UUID lookups,
then optionally drops the UUID column.

## Usage

``` r
replace_uuid_with_id(
  con,
  table_name,
  uuid_col,
  new_id_col,
  ref_table,
  ref_uuid_col,
  ref_id_col,
  drop_uuid = TRUE
)
```

## Arguments

- con:

  DuckDB connection

- table_name:

  Table to update

- uuid_col:

  Name of UUID column to replace

- new_id_col:

  Name of new integer ID column

- ref_table:

  Reference table containing UUID-to-ID mapping

- ref_uuid_col:

  UUID column in reference table

- ref_id_col:

  ID column in reference table

- drop_uuid:

  Whether to drop the UUID column after replacement (default: TRUE)

## Value

Invisibly returns the connection

## Examples

``` r
if (FALSE) { # \dontrun{
# replace site_uuid with site_id FK
replace_uuid_with_id(
  con         = con,
  table_name  = "tow",
  uuid_col    = "site_uuid",
  new_id_col  = "site_id",
  ref_table   = "site",
  ref_uuid_col = "site_uuid",
  ref_id_col  = "site_id")
} # }
```
