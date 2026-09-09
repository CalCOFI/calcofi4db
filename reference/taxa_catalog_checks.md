# The checks [`check_taxa_catalog()`](https://calcofi.io/calcofi4db/reference/check_taxa_catalog.md) runs, with their level

Every `error` finding stops the release: the record is generated, so a
failure is a bug in the generator or a break in the release's own
tables, never something a registry row can excuse.

## Usage

``` r
taxa_catalog_checks()
```

## Value

A named character vector, check -\> level.
