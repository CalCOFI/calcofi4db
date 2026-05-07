# Assign deterministic UUIDs from composite key columns

Generates UUID v5 (name-based SHA-1) identifiers from a composite key.
The same key values always produce the same UUID, making IDs stable
across re-ingestion regardless of row order.

## Usage

``` r
assign_deterministic_uuids(
  con,
  table_name,
  id_col,
  key_cols,
  namespace_uuid = "c0f1ca00-ca1c-5000-b000-1c4790000000",
  chunk_size = 5e+06
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

- namespace_uuid:

  Fixed namespace UUID for deterministic generation

- chunk_size:

  Number of rows per chunk for large tables (default: 5e6)

## Value

Invisibly returns the connection after adding UUID column

## Details

For large tables (over `chunk_size` rows), processes in chunks to avoid
exceeding R memory limits. Only key columns are read per chunk, and
UUIDs are written back via temp table UPDATE joins.
