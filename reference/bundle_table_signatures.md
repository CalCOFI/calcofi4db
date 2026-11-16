# Row signatures of a dataset's bundle tables, for its fingerprint

The bundle is a function of exactly the rows written, so these — not the
release objects they were cut from — are its data inputs: a vocabulary
row another dataset added, or a release that left this dataset alone,
changes nothing here. Each table is signed by the release's own
row-signature rule (order-independent per-column hash sums, the identity
[`freeze_plan()`](https://calcofi.io/calcofi4db/reference/freeze_plan.md)
uses).

## Usage

``` r
bundle_table_signatures(con, tables, prefix = "bundle_")
```

## Arguments

- con:

  the connection
  [`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)
  wrote into

- tables:

  the table names (without prefix)

- prefix:

  the prefix
  [`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)
  used

## Value

A named character vector, `data:{table}` -\> md5, for
[`publish_fingerprint()`](https://calcofi.io/calcofi4db/reference/publish_fingerprint.md).
