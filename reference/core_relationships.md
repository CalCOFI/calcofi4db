# PK/FK spec for the consolidated core tables

Every ingest now emits the same core shape, so every ingest declares the
same relationships. This returns that one spec in
[`build_relationships_json()`](https://calcofi.io/calcofi4db/reference/build_relationships_json.md)'s
`rels` form, restricted to the tables actually present in `tables` — so
an ingest that emits no `obs_attribute` does not advertise an edge to
it.

## Usage

``` r
core_relationships(tables)
```

## Arguments

- tables:

  character vector of tables the ingest writes (typically the result of
  [`core_output_tables()`](https://calcofi.io/calcofi4db/reference/core_output_tables.md))

## Value

a list with `primary_keys` and `foreign_keys`
