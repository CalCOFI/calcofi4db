# Check one dataset's EML document

The schema half is
[`EML::eml_validate()`](https://docs.ropensci.org/emld/reference/eml_validate.html)
— EML 2.2's XSDs ship with emld, so nothing here touches the network.
The rest is the required-element checklist EDI's evaluate applies (see
[`eml_findings()`](https://calcofi.io/calcofi4db/reference/eml_findings.md)),
read off the built document rather than off the record, so what is
asserted is what was written.

## Usage

``` r
check_eml(doc, path = NULL, record = NULL, validate = TRUE)
```

## Arguments

- doc:

  an EML document from
  [`build_eml()`](https://calcofi.io/calcofi4db/reference/build_eml.md),
  or a path to a written `.xml` (the fallback notes are only available
  from the document)

- path:

  the written file to validate; defaults to `doc` when `doc` is a path

- record:

  the dataset's catalog record, for the question-row exemptions

- validate:

  run
  [`EML::eml_validate()`](https://docs.ropensci.org/emld/reference/eml_validate.html)
  (default TRUE)

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html):
`dataset_key`, `finding`, `level`, `detail`, `exempt`, `question` — the
shape
[`check_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/check_dataset_catalog.md)
returns.

## See also

[`assert_eml()`](https://calcofi.io/calcofi4db/reference/assert_eml.md)
