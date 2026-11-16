# The column dictionary of a dataset's bundle

One row per column of every table in the bundle: the type it is written
with, its units and its meaning, from the release's own `metadata.json`
(`columns{"table.column"}`); the columns
[`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)
adds are described by this package.

## Usage

``` r
bundle_columns(con, tables, meta_columns = list(), prefix = "bundle_")
```

## Arguments

- con:

  the connection
  [`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)
  wrote into

- tables:

  the bundle's table names, in presentation order

- meta_columns:

  `metadata.json`'s `columns` block (a named list)

- prefix:

  the prefix
  [`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)
  used

## Value

A data frame: `table`, `column`, `type`, `units`, `description`.
