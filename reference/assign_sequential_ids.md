# Assign Sequential IDs with Deterministic Sort Order

Assigns sequential integer IDs to a table based on specified sort order.
This ensures reproducibility when appending new data - the same data
will always get the same IDs.

## Usage

``` r
assign_sequential_ids(con, table_name, id_col = NULL, sort_cols, start_id = 1)
```

## Arguments

- con:

  DuckDB connection

- table_name:

  Name of table to assign IDs to

- id_col:

  Name of ID column to create (default: paste0(table_name, "\_id"))

- sort_cols:

  Character vector of columns to sort by for ID assignment

- start_id:

  Starting ID value (default: 1)

## Value

Invisibly returns the connection after adding ID column

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_duckdb_con()

# assign site_id sorted by cruise_key and orderocc
assign_sequential_ids(
  con        = con,
  table_name = "site",
  id_col     = "site_id",
  sort_cols  = c("cruise_key", "orderocc"))

# assign lookup_id with multi-column sort
assign_sequential_ids(
  con        = con,
  table_name = "lookup",
  id_col     = "lookup_id",
  sort_cols  = c("lookup_type", "lookup_num"))
} # }
```
