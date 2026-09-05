# Build the Darwin Core Event core for one dataset

From `sample`'s adjacency list — the same rows every consumer reads —
plus the `cruise` reference as the root event, plus the effort
denominator from `sample_measurement`:

## Usage

``` r
dwc_event(
  con,
  dataset_key,
  gear = NULL,
  measurement_type = NULL,
  cruises = TRUE,
  close_tree = TRUE
)
```

## Arguments

- con:

  a DBI connection to the release

- dataset_key:

  the dataset

- gear:

  the registry from
  [`read_gear_registry()`](https://calcofi.io/calcofi4db/reference/read_gear_registry.md),
  or NULL (no `samplingProtocol`)

- measurement_type:

  the registry from
  [`read_measurement_type()`](https://calcofi.io/calcofi4db/reference/read_measurement_type.md),
  or NULL (`sampleSizeUnit` is then empty)

- cruises:

  emit a cruise root event per `cruise_key` and parent the roots to it

- close_tree:

  pull in an ancestor event that belongs to ANOTHER dataset, so the
  archive's `parentEventID`s all resolve. `sample_key` is globally
  unique and a `parent_sample_key` may point across datasets —
  `cdfw_dungeness-crab` parents 306 examined subsamples onto
  `swfsc_ichthyo` **site** occupations, and `calcofi_dic` parents 6
  bottles onto `calcofi_bottle` casts (that is how the DIC/bottle dedup
  works). In the release those are not orphans; in a single-dataset
  archive they would be, so the ancestors travel with their own
  `datasetID` rather than the pointer being dropped.

## Value

A data frame of Event-core rows, all-NA columns dropped.

## Details

|  |  |
|----|----|
| Darwin Core | core |
| `eventID` | `sample.sample_key` |
| `parentEventID` | `sample.parent_sample_key`, or the row's `cruise_key` for a root |
| `eventType` | `sample.sample_type` (`"cruise"` for a cruise event) |
| `eventDate` | `sample.datetime` as ISO 8601 UTC; a cruise's `date_min/date_max` span |
| `decimalLatitude` / `decimalLongitude` | `sample.latitude` / `sample.longitude` |
| `minimumDepthInMeters` / `maximumDepthInMeters` | `sample.depth_min_m` / `depth_max_m` |
| `locationID` | `sample.site_key` |
| `samplingProtocol` | `gear.csv` `dwc_samplingProtocol` for `sample.tow_type` |
| `sampleSizeValue` / `sampleSizeUnit` | `sample_measurement` `volume_sampled` + its registry unit |
| `geodeticDatum` | `"WGS84"` — the release's own CRS |
| `datasetID` | `dataset_key` |

**Cruise events.** A dataset's root samples carry `cruise_key` but no
parent, so `cruises = TRUE` (the default) emits one event per cruise the
dataset touches and parents the roots to it. That is a derivation from a
column already on the row, not an invention, and it is what makes an
archive's events group the way a reader expects. `cruises = FALSE`
leaves the roots parentless.

Nothing here asserts `countryCode`, `waterBody` or
`coordinateUncertaintyInMeters`: the release measures none of them, and
the ichthyo notebook's hand-typed values were dataset metadata living
where no provider could edit them (D-8).

## See also

[`dwc_occurrence()`](https://calcofi.io/calcofi4db/reference/dwc_occurrence.md),
[`dwc_emof()`](https://calcofi.io/calcofi4db/reference/dwc_emof.md),
[`dwc_archive()`](https://calcofi.io/calcofi4db/reference/dwc_archive.md)
