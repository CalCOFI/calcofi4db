# Write `taxa.json`

Minified, not pretty-printed: the record is ~2.4 MB on v2026.09.06 and
is read by a build, never by a person. `na = "null"` is what turns the
record's `NA` scalars into the JSON `null` the schema declares.

## Usage

``` r
write_taxa_catalog(record, dir)
```

## Arguments

- record:

  from
  [`build_taxa_catalog()`](https://calcofi.io/calcofi4db/reference/build_taxa_catalog.md)

- dir:

  the release directory (created if missing)

## Value

The path written, invisibly.

## See also

[`build_taxa_catalog()`](https://calcofi.io/calcofi4db/reference/build_taxa_catalog.md)
