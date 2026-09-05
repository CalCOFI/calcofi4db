# Write `eml/{dataset_key}.xml` for every built document

[`EML::write_eml()`](https://docs.ropensci.org/EML/reference/write_eml.html)
renders the document; the file lands under `eml/` in the release
directory beside `datasets.json`.

## Usage

``` r
write_eml_files(docs, dir)
```

## Arguments

- docs:

  the named list from
  [`build_eml_catalog()`](https://calcofi.io/calcofi4db/reference/build_eml_catalog.md)
  (or one
  [`build_eml()`](https://calcofi.io/calcofi4db/reference/build_eml.md)
  document)

- dir:

  the release directory (an `eml/` subdirectory is created)

## Value

A named character vector of paths, keyed by `dataset_key`, invisibly.
