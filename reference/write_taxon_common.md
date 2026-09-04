# Write the vernacular-name registry

The one writer for `metadata/taxon_common.csv`: fixed column order,
sorted by `taxon_key`, `na = ""` so an empty cell never round-trips as
the string `"NA"`. Use it instead of a bare
[`write.csv()`](https://rdrr.io/r/utils/write.table.html) after editing
a `common_name`.

## Usage

``` r
write_taxon_common(cache, path)
```

## Arguments

- cache:

  the registry (as read by
  [`read_taxon_common()`](https://calcofi.io/calcofi4db/reference/read_taxon_common.md))

- path:

  where to write it

## Value

`path`, invisibly
