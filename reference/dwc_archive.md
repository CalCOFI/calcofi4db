# Write a Darwin Core Archive and its manifest

Writes `event.csv`, `occurrence.csv`, `extendedMeasurementOrFact.csv`,
`meta.xml` and `eml.xml` into `dir`, zips them (flat, no directory
entries — what the IPT expects), and writes `manifest.json` beside the
zip.

## Usage

``` r
dwc_archive(
  dir,
  event,
  occurrence = NULL,
  emof = NULL,
  eml_path = NULL,
  dataset_key = NA_character_,
  version = NA_character_,
  ipt_resource = NULL,
  obis_dataset_id = NULL,
  zip_path = NULL
)
```

## Arguments

- dir:

  the directory to write into (created)

- event, occurrence, emof:

  the checked frames

- eml_path:

  path to the release's `eml/{dataset_key}.xml`, copied in as `eml.xml`;
  NULL writes no metadata document (and says so)

- dataset_key, version:

  named in the manifest and the zip name

- ipt_resource, obis_dataset_id:

  the published copy's ids, from `distribution.csv`; NULL keeps whatever
  an existing manifest holds

- zip_path:

  where to write the archive; defaults to
  `{dirname(dir)}/{dataset_key}_{version}.zip`

## Value

A list: `zip`, `manifest` (the path), `content_hash`, `counts`.

## Details

**The manifest is how a dataset page knows an upload is due** (D-8): it
records `content_hash` (an md5 over the three CSVs, deterministic — the
same rows always hash the same), the release `version`, and the
`ipt_resource` / `obis_dataset_id` / `uploaded_utc` of the *published*
copy. Those last three are never invented here: they are carried forward
from an existing `manifest.json` (or supplied by the caller from
`distribution.csv`), so a freshly built archive that has never been
uploaded says `uploaded_utc: null`, and `registrations[]` reads that as
"not published" rather than as a date nobody set.

The upload itself is a deliberate manual act through the OBIS-USA IPT
(Decision 10, `docs/portals.qmd` § OBIS). Nothing in this function talks
to a portal.

## See also

[`dwc_check()`](https://calcofi.io/calcofi4db/reference/dwc_check.md),
[`dwc_meta_xml()`](https://calcofi.io/calcofi4db/reference/dwc_meta_xml.md)
