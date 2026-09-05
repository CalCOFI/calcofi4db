# What ERDDAP serves now: `allDatasets` as a table

What ERDDAP serves now: `allDatasets` as a table

## Usage

``` r
fetch_erddap_datasets(base = CC_ERDDAP_BASE, fetch = NULL)

parse_erddap_all_datasets(x)
```

## Arguments

- base:

  the ERDDAP base URL (`…/erddap`)

- fetch:

  the HTTP function (see
  [`check_dataset_citation()`](https://calcofi.io/calcofi4db/reference/check_dataset_citation.md));
  the tests inject one that serves a saved CSV

- x:

  the CSV text of `allDatasets.csv?datasetID,title`

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html)
`datasetID`, `title` — the `allDatasets` row itself dropped — or NULL
when the server did not answer.
