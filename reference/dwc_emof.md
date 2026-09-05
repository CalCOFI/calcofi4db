# Build the ExtendedMeasurementOrFact extension for one dataset

Three grains, all with `measurementTypeID` / `measurementUnitID` from
`measurement_type.csv`'s `nerc_p01` / `units_nerc_p06` — **empty where
the registry states no exact concept, never invented**:

## Usage

``` r
dwc_emof(
  con,
  dataset_key,
  occurrence = NULL,
  measurement_type = NULL,
  env = TRUE
)
```

## Arguments

- con:

  a DBI connection to the release

- dataset_key:

  the dataset

- occurrence:

  the frame from
  [`dwc_occurrence()`](https://calcofi.io/calcofi4db/reference/dwc_occurrence.md)
  — needed to resolve `occurrenceID`; without it the `obs_attribute`
  grain is skipped

- measurement_type:

  the registry from
  [`read_measurement_type()`](https://calcofi.io/calcofi4db/reference/read_measurement_type.md),
  or NULL

- env:

  include `obs_env` rows on the dataset's own events

## Value

A data frame of eMoF rows, all-NA columns dropped.

## Details

- **event** — `sample_measurement` rows for the dataset, minus
  `volume_sampled` (which is the event's `sampleSizeValue`, not a repeat
  measurement).

- **occurrence** — `obs_attribute` rows, joined to their occurrence on
  `(sample_key, taxon_key, life_stage)`. Which of a bin's two numbers is
  the `measurementValue` is decided by the REGISTRY, not per dataset: a
  type with a physical `units` (`body_length` mm, `carapace_length` mm)
  puts `bin_value` in `measurementValue` and the bin's `count` in
  `measurementRemarks`; a type with no unit (`stage`, `behavior`) is a
  categorical bin, so the `count` is the value and the `bin_label` is
  the remark.

- **event, environmental** — `obs_env` rows sitting on one of this
  dataset's own events (`sample_key`). Empty for a dataset that measures
  no environment itself.

## See also

[`dwc_event()`](https://calcofi.io/calcofi4db/reference/dwc_event.md),
[`dwc_occurrence()`](https://calcofi.io/calcofi4db/reference/dwc_occurrence.md)
