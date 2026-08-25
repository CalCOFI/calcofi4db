# Fail unless every core table is unique on its primary key

The release gate that was missing when v2026.08.25 shipped `sample` with
4,855 keys twice: the `validate` chunk only *warned* on `ship`/`cruise`.
Primary keys come from
[`core_relationships()`](https://calcofi.io/calcofi4db/reference/core_relationships.md).

## Usage

``` r
check_core_pk_unique(con, tables)
```

## Arguments

- con:

  DuckDB connection holding the assembled release

- tables:

  core tables to check (those present in `con` are checked)

## Value

invisibly, a data.frame `table`, `pk`, `n_rows`, `n_dup`
