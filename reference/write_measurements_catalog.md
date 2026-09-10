# Write `measurements.json`

Minified, not pretty-printed: the record is read by a build, never by a
person. `na = "null"` is what turns the record's `NA` scalars into the
JSON `null` the schema declares.

## Usage

``` r
write_measurements_catalog(record, dir)
```

## Arguments

- record:

  from
  [`build_measurements_catalog()`](https://calcofi.io/calcofi4db/reference/build_measurements_catalog.md)

- dir:

  the release directory (created if missing)

## Value

The path written, invisibly.

## See also

[`build_measurements_catalog()`](https://calcofi.io/calcofi4db/reference/build_measurements_catalog.md)
