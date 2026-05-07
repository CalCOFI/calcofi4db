# Assign deterministic UUIDs using DuckDB-native md5

Generates UUID-style identifiers from a composite key entirely inside
DuckDB using `md5()`. Unlike
[`assign_deterministic_uuids()`](https://calcofi.io/calcofi4db/reference/assign_deterministic_uuids.md)
which pulls data into R and uses UUID v5 (SHA-1), this runs as a single
SQL statement with zero R data transfer — orders of magnitude faster on
large tables.

## Usage

``` r
assign_deterministic_uuids_md5(
  con,
  table_name,
  id_col,
  key_cols,
  namespace = NULL
)
```

## Arguments

- con:

  DuckDB connection

- table_name:

  Name of table to assign UUIDs to

- id_col:

  Name of UUID column to create

- key_cols:

  Character vector of columns forming the composite key

- namespace:

  Optional namespace string prepended to keys for domain separation
  (default: NULL)

## Value

Invisibly returns the connection after adding UUID column

## Details

The 32-char md5 hex is formatted as 8-4-4-4-12 to resemble a UUID
string. These are **not** RFC 4122 UUIDs — they are md5-based internal
identifiers. Use this for internal-only IDs on large tables (e.g.,
ctd_measurement). Use
[`assign_deterministic_uuids()`](https://calcofi.io/calcofi4db/reference/assign_deterministic_uuids.md)
when RFC 4122 UUID v5 compatibility is needed (e.g., ichthyo tables
shared with external systems).
