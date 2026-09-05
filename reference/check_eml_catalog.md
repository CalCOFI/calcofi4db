# Check every dataset's EML document

Check every dataset's EML document

## Usage

``` r
check_eml_catalog(docs, paths = NULL, catalog = NULL)
```

## Arguments

- docs:

  the named list from
  [`build_eml_catalog()`](https://calcofi.io/calcofi4db/reference/build_eml_catalog.md)

- paths:

  the named character vector from
  [`write_eml_files()`](https://calcofi.io/calcofi4db/reference/write_eml_files.md)
  (NULL: no schema validation)

- catalog:

  the catalog record, for each dataset's question rows

## Value

One [tibble](https://tibble.tidyverse.org/reference/tibble.html) over
every dataset.
