# Derive measurement_type → contributing datasets from the data

Scans every measurement-bearing table (those with a `measurement_type`
column) for its distinct measurement types and unions in the dataset(s)
that own the table, yielding the true one-to-many map (e.g.
`temperature` reported by both `calcofi_bottle` and `calcofi_ctd-cast`).
Feed the result to
[`merge_metadata_json()`](https://calcofi.io/calcofi4db/reference/merge_metadata_json.md)
via its `measurement_datasets` argument so the schema site's
per-measurement dataset filter reflects reality rather than the single
"first-defined" dataset.

## Usage

``` r
derive_measurement_type_datasets(con, table_datasets, from_fn = identity)
```

## Arguments

- con:

  A DBI connection (e.g. from
  [`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md)).
  Reads happen via `from_fn(table)` so the same helper works against
  in-DB tables or remote parquet over httpfs.

- table_datasets:

  Named list, `table -> character vector of provider_datasets`.
  Typically built from the ingest YAML `tables_owned` or the metadata
  `contributions` block.

- from_fn:

  Function mapping a table name to a SQL FROM source. Defaults to the
  identity (the table exists in `con`); for remote parquet pass e.g.
  `function(t) sprintf("read_parquet('%s/%s.parquet')", base, t)`.

## Value

Named list,
`measurement_type -> sorted unique character vector of provider_datasets`.

## Details

Tables in `table_datasets` that lack a `measurement_type` column are
skipped silently. Exclude the `measurement_type` vocabulary/lookup table
from `table_datasets` — its `measurement_type` column is the term list,
not measured rows, so including it would mis-attribute every term to
every contributing dataset.

## Examples

``` r
if (FALSE) { # \dontrun{
td <- list(bottle_measurement = "calcofi_bottle",
           ctd_thin           = "calcofi_ctd-cast")
derive_measurement_type_datasets(con, td)
} # }
```
