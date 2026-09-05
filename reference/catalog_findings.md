# The findings `check_dataset_catalog()` can report, with their level

`error` findings fail the release unless exempt (only `no_citation` can
be: an open/proposed `questions.csv` row on `related_table = dataset`
naming `citation_main`, or no field, covers it — the citation contract's
rule); `warn` findings never block.

## Usage

``` r
catalog_findings()
```

## Value

A named character vector, finding -\> level.
