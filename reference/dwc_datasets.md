# The biological datasets a Darwin Core Archive can be built for

Decision 21 (Ben, 2026-09-05): **one IPT resource per dataset whose taxa
resolve to WoRMS**. This measures that from the release rather than
listing it: a dataset is a candidate when it has rows in `obs_bio` and
at least one of the taxa it observed carries a WoRMS id.
`cce-lter_picoplankton-bacteria` (flow-cytometry groups, environmental
realm) and `sio_pic-zooplankton` (no taxa) have no `obs_bio` rows at
all, so they fall out by construction rather than by a hard-coded
exclusion.

## Usage

``` r
dwc_datasets(con)
```

## Arguments

- con:

  a DBI connection to the release (`calcofi4r::cc_get_db()`)

## Value

A data frame, one row per candidate dataset: `dataset_key`, `n_obs`,
`n_taxa`, `n_worms`, `n_no_worms`, `n_no_taxon`, `absence_rule`.

## Details

`n_no_worms` is the count of observed taxa with no WoRMS id: those
occurrences ship with `scientificNameID` empty (never a guessed LSID),
and
[`dwc_check()`](https://calcofi.io/calcofi4db/reference/dwc_check.md)
reports it.

## See also

[`dwc_absence_rule()`](https://calcofi.io/calcofi4db/reference/dwc_absence_rule.md),
[`dwc_event()`](https://calcofi.io/calcofi4db/reference/dwc_event.md),
[`dwc_occurrence()`](https://calcofi.io/calcofi4db/reference/dwc_occurrence.md)
