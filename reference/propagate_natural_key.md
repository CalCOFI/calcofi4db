# Propagate Key from Parent to Child Table

Copies a key column from a parent table to a child table by joining on a
common column (typically UUID). This is used to populate foreign key
columns before assigning sequential IDs with deterministic sort order.

## Usage

``` r
propagate_natural_key(
  con,
  child_tbl,
  parent_tbl,
  key_col,
  join_col,
  key_type = NULL
)
```

## Arguments

- con:

  DuckDB connection

- child_tbl:

  Name of child table to update

- parent_tbl:

  Name of parent table containing the key

- key_col:

  Name of the key column to propagate (e.g., "cruise_key", "site_id")

- join_col:

  Name of the column to join on (must exist in both tables)

- key_type:

  SQL data type for the key ("TEXT" or "INTEGER", default: auto-detect
  from parent)

## Value

Invisibly returns the connection after adding and populating key column

## Examples

``` r
if (FALSE) { # \dontrun{
# propagate cruise_key (TEXT) to site table
propagate_natural_key(
  con        = con,
  child_tbl  = "site",
  parent_tbl = "cruise",
  key_col    = "cruise_key",
  join_col   = "cruise_uuid")

# propagate site_id (INTEGER) to tow table
propagate_natural_key(
  con        = con,
  child_tbl  = "tow",
  parent_tbl = "site",
  key_col    = "site_id",
  join_col   = "site_uuid")
} # }
```
