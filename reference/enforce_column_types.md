# Enforce Column Types Before Export

Runs `ALTER TABLE ... ALTER COLUMN ... TYPE` for every column whose
current DuckDB type differs from the target type. Intended to run right
before
[`write_parquet_outputs()`](https://calcofi.io/calcofi4db/reference/write_parquet_outputs.md)
so that parquet files carry correct integer/decimal types instead of
DOUBLE from R's default numeric mapping.

## Usage

``` r
enforce_column_types(
  con,
  d_flds_rd = NULL,
  type_overrides = NULL,
  tables = NULL,
  verbose = TRUE
)
```

## Arguments

- con:

  DuckDB connection

- d_flds_rd:

  Field redefinition data frame with columns `tbl_new`, `fld_new`,
  `type_new` (optional; NULL skips this source)

- type_overrides:

  Named list of `table.column = "SQL_TYPE"` overrides for derived-table
  columns (optional; NULL skips)

- tables:

  Character vector of tables to enforce. If NULL, uses all tables from
  `DBI::dbListTables(con)`.

- verbose:

  Print messages for each change (default: TRUE)

## Value

Tibble of changes applied (table, column, from_type, to_type, success)

## Details

Target types come from two sources:

1.  `d_flds_rd` (field redefinition data frame) — keyed by
    `tbl_new.fld_new` with target in `type_new`

2.  `type_overrides` — named list for derived-table columns not in the
    redefinition file, e.g. `list(ichthyo.species_id = "SMALLINT")`

## Examples

``` r
if (FALSE) { # \dontrun{
changes <- enforce_column_types(
  con            = con,
  d_flds_rd      = d$d_flds_rd,
  type_overrides = list(
    ichthyo.species_id = "SMALLINT",
    ichthyo.tally      = "INTEGER"),
  verbose        = TRUE)
} # }
```
