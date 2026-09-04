# The findings `check_dataset_citation()` can report, with their level

`error` findings fail the workflows index and the release unless an
`open`/`proposed` `questions.csv` row on the dataset covers the field;
`warn` findings are reported and never block.

## Usage

``` r
citation_findings()

citation_error_findings()
```

## Value

A named character vector, finding -\> `"ok"` \| `"error"` \| `"warn"`.
