# The findings [`dwc_check()`](https://calcofi.io/calcofi4db/reference/dwc_check.md) can report, with their level

`error` findings mean **no archive is written** for that dataset — the
whole point of gating: a broken archive at OBIS is worse than a missing
one.

## Usage

``` r
dwc_findings()
```

## Value

A named character vector, finding -\> level.

## Details

- `orphan_event` — a `parentEventID` naming no event in the core
  ([`obistools::check_eventids()`](https://rdrr.io/pkg/obistools/man/check_eventids.html)).

- `orphan_occurrence` — an occurrence whose `eventID` is not in the
  core.

- `orphan_emof` — an eMoF row whose `eventID` / `occurrenceID` is not in
  the archive.

- `missing_required_field` —
  [`obistools::check_fields()`](https://rdrr.io/pkg/obistools/man/check_fields.html)
  at level `error` on **every** occurrence: nothing in the archive would
  index at OBIS, so writing it would publish an empty dataset.
  `calcofi_phytoplankton` is here at v2026.09.05 — all 409 `region_pool`
  samples carry no `datetime`, so no occurrence has an `eventDate`.

- `incomplete_records` — the same check failing on SOME occurrences:
  those records will not index at OBIS and the rest will, so the archive
  is written and the count is reported. A gap in the release, not a
  fault of the mapping.

- `bad_event_date` —
  [`obistools::check_eventdate()`](https://rdrr.io/pkg/obistools/man/check_eventdate.html)
  rejected a value.

- `duplicate_id` — a repeated `eventID` or `occurrenceID`.

- `no_occurrence` — the dataset produced no occurrence rows at all.

- `no_scientific_name_id` — occurrences whose taxon has no WoRMS id, so
  `scientificNameID` is empty (warn — never a guessed LSID).

- `no_life_stage_id`, `no_measurement_type_id`, `no_measurement_unit_id`
  — a registry states no exact concept for a value the archive emits
  (warn).

- `dropped_no_taxon` — `obs_bio` rows with no taxon, which cannot be
  Occurrences (warn).

- `no_event_date`, `no_coordinates` — events missing either (warn: OBIS
  accepts the archive, a consumer will notice).
