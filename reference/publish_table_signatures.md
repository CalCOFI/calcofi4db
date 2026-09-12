# Per-dataset row signatures of tables already in a connection

The local twin of
[`publish_object_signatures()`](https://calcofi.io/calcofi4db/reference/publish_object_signatures.md),
for a publisher that materializes the core before building (the OBIS and
netCDF publishers do, to join across tables): each table's rows grouped
by `dataset_key` and signed. A table with no `dataset_key` column is
signed per dataset through `via` — the rows a dataset actually reaches
by a key — so a vocabulary row another dataset added does not rebuild
this one; without a `via` it is signed whole (`dataset_key = "*"`). A
table absent from the connection contributes nothing, which
[`publish_data_parts()`](https://calcofi.io/calcofi4db/reference/publish_data_parts.md)
reports as `"<absent>"`.

## Usage

``` r
publish_table_signatures(con, tables, via = list())
```

## Arguments

- con:

  a DuckDB connection holding the tables

- tables:

  table (or view) names

- via:

  named list, table -\> `c(from, key)`: sign `table`'s rows joined to
  the distinct (`dataset_key`, `key`) pairs of table `from`, e.g.
  `list(taxon = c("obs_bio", "taxon_key"), cruise = c("sample", "cruise_key"))`

## Value

The data frame
[`publish_object_signatures()`](https://calcofi.io/calcofi4db/reference/publish_object_signatures.md)
returns, `content_hash` empty.
