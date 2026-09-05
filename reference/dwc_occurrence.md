# Build the Darwin Core Occurrence extension for one dataset

From `obs_bio` joined to `taxon`, with `lifeStage` from `life_stage.csv`
and the D8 denominator as `organismQuantity`:

## Usage

``` r
dwc_occurrence(
  con,
  dataset_key,
  life_stage = NULL,
  measurement_type = NULL,
  absences = c("none", "sample_root"),
  max_absences = 5e+06
)
```

## Arguments

- con:

  a DBI connection to the release

- dataset_key:

  the dataset

- life_stage:

  the registry from
  [`read_life_stage_registry()`](https://calcofi.io/calcofi4db/reference/read_life_stage_registry.md),
  or NULL

- measurement_type:

  the registry from
  [`read_measurement_type()`](https://calcofi.io/calcofi4db/reference/read_measurement_type.md),
  or NULL

- absences:

  `"none"` or `"sample_root"`

- max_absences:

  refuse to derive more absences than this

## Value

A data frame of Occurrence rows, all-NA columns dropped.

## Details

|  |  |
|----|----|
| Darwin Core | core |
| `occurrenceID` | md5 of `(sample_key, taxon_key, life_stage, measurement_type, depth_bin, ordinal)` — stable across releases (`obs_id` is not) |
| `eventID` | `obs_bio.sample_key`, or the root's `sample_key` for a derived absence |
| `scientificName` / `taxonID` / `taxonRank` / lineage | `taxon` |
| `scientificNameID` | the WoRMS LSID of `taxon.worms_id`; **empty when the taxon has none** |
| `lifeStage` | `life_stage.csv` `dwc_lifeStage`, else the verbatim value where the registry gives it a `life_stage_parent`; a value the registry records as *not a life stage* goes to `occurrenceRemarks` |
| `organismQuantity` / `organismQuantityType` | `density_per_10m2`, else `density_per_1000m3`, else `value` + its registry `units` |
| `occurrenceStatus` | `"present"` where `value > 0`, `"absent"` where `value = 0` |

**The absence rule.** See
[`dwc_absence_rule()`](https://calcofi.io/calcofi4db/reference/dwc_absence_rule.md).
`absences = "none"` (the default) emits only rows the release holds.
`absences = "sample_root"` additionally emits one `absent` row for every
(surveyed root sample x observed taxon/stage) pair with no positive row
— the `sample_root` minus positives rule. It is correct only for a
dataset whose protocol sorts every sample for its whole vocabulary, so
it is never the default, and `max_absences` (5,000,000) stops a large
vocabulary from turning a survey into a hundred million assertions
nobody made.

## See also

[`dwc_event()`](https://calcofi.io/calcofi4db/reference/dwc_event.md),
[`dwc_emof()`](https://calcofi.io/calcofi4db/reference/dwc_emof.md),
[`dwc_absence_rule()`](https://calcofi.io/calcofi4db/reference/dwc_absence_rule.md)
