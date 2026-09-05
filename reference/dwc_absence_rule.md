# Which `occurrenceStatus` rule a dataset falls under

Measured from the data, never declared:

## Usage

``` r
dwc_absence_rule(con, dataset_key)
```

## Arguments

- con:

  a DBI connection to the release

- dataset_key:

  the dataset

## Value

`"zeros_recorded"` or `"positive_only"`.

## Details

- `"zeros_recorded"` — the dataset has zero-valued `obs_bio` rows, so a
  sample that was examined and held none of a taxon is already in the
  release. Those rows become `occurrenceStatus = "absent"` and nothing
  is derived.

- `"positive_only"` — the dataset has no zero rows: a surveyed-empty
  sample simply has no row. `occurrenceStatus` is `"present"` for every
  row it does have, and an absence exists only if the protocol sorted
  every sample for the whole vocabulary — a claim about the protocol
  that the release cannot make.
  [`dwc_occurrence()`](https://calcofi.io/calcofi4db/reference/dwc_occurrence.md)
  therefore derives absences ONLY when asked
  (`absences = "sample_root"`).
