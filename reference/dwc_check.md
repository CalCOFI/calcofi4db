# Check an archive's three tables before it is written

Runs the `obistools` gate (`check_eventids()`,
`check_extension_eventids()`, `check_fields()`, `check_eventdate()`)
plus the referential and controlled-vocabulary checks the release can
make itself. No network: taxon names are checked against the release's
own `taxon` table, not
[`obistools::match_taxa()`](https://rdrr.io/pkg/obistools/man/match_taxa.html),
which is an interactive WoRMS call.

## Usage

``` r
dwc_check(event, occurrence = NULL, emof = NULL, dataset_key = NA_character_)
```

## Arguments

- event, occurrence, emof:

  the frames from
  [`dwc_event()`](https://calcofi.io/calcofi4db/reference/dwc_event.md)
  /
  [`dwc_occurrence()`](https://calcofi.io/calcofi4db/reference/dwc_occurrence.md)
  / [`dwc_emof()`](https://calcofi.io/calcofi4db/reference/dwc_emof.md)

- dataset_key:

  named in the findings

## Value

A data frame: `dataset_key`, `finding`, `level`, `n`, `detail`. One `ok`
row when nothing is found.

## See also

[`assert_dwc()`](https://calcofi.io/calcofi4db/reference/assert_dwc.md),
[`dwc_findings()`](https://calcofi.io/calcofi4db/reference/dwc_findings.md)
