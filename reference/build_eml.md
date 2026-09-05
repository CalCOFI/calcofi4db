# Build one dataset's EML 2.2 document from the catalog record

Plan § D-8: `eml/{dataset_key}.xml` in every release, generated from the
record and the descriptive sidecar so the DwC-A, the EDI package,
ERDDAP's globals and the page's JSON-LD all read one document. The
mapping, field by field:

## Usage

``` r
build_eml(
  record,
  sidecar = NULL,
  meta = NULL,
  coverage = NULL,
  release = NULL,
  gear = NULL
)
```

## Arguments

- record:

  one dataset record (an element of `datasets.json`'s `datasets[]`)

- sidecar:

  the dataset's `dataset_meta.yml` as a list (the narrative fields —
  `methods_md`, `study_extent`, `sampling_description`,
  `quality_control_md`, `maintenance`, `associated_parties` — live only
  there)

- meta:

  the release `metadata.json` (path or parsed list), for `columns{}`

- coverage:

  the release `coverage.json` (path or parsed list), for `taxa[]`

- release:

  the `release` block of `datasets.json` (version, date, citation);
  defaults to the record's own `release` element when it carries one

- gear:

  the tibble from
  [`read_gear_registry()`](https://calcofi.io/calcofi4db/reference/read_gear_registry.md),
  or NULL

## Value

A named list ready for
[`EML::write_eml()`](https://docs.ropensci.org/EML/reference/write_eml.html),
carrying an `"eml_notes"` attribute (which fallbacks were used) that
[`check_eml()`](https://calcofi.io/calcofi4db/reference/check_eml.md)
reads.

## Details

|  |  |
|----|----|
| EML | from |
| `packageId` / `system` | `{dataset_key}.{release version}` / `calcofi.io` |
| `dataset/alternateIdentifier` | `attribution.doi` (as a DOI URL) and `links.page` |
| `dataset/shortName` · `title` | `dataset_name_short` · `dataset_name` |
| `dataset/creator` | `attribution.creators[]`, else `pi_names` with the provider organization, else the provider organization alone |
| `dataset/pubDate` | the release `release_date` |
| `dataset/language` | `eng` |
| `dataset/abstract` | `description_md`, rendered to paragraphs |
| `dataset/keywordSet` | `keywords` — the GCMD terms under their thesaurus, plus the category and the observed variable names |
| `dataset/intellectualRights` · `licensed` | `attribution.license` / `license_name` / `license_url` (`metadata/license.csv`) |
| `dataset/distribution` | `links.page` |
| `dataset/coverage` | geographic from `coverage.bbox` + `coverage.spatial`; temporal from `coverage.year_min/max`; taxonomic from `coverage.json`'s `taxa[]` for this dataset (WoRMS / ITIS ids) |
| `dataset/maintenance` | the sidecar's `maintenance` |
| `dataset/contact` | `attribution.contact`, else a creator email, else [`eml_contact_address()`](https://calcofi.io/calcofi4db/reference/eml_contact_address.md) |
| `dataset/methods` | the sidecar's `methods_md`, `quality_control_md`, `study_extent`, `sampling_description`, with `metadata/gear.csv`'s `dwc_samplingProtocol` sentences for the dataset's `tow_type`s |
| `dataset/project` | `dataset_name` + `attribution.funding` (else `acknowledgement`), personnel from the creators |
| `dataset/dataTable[]` | the record's `tables[]`, `attributeList` from `metadata.json`'s `columns{}` (label, definition, units, storage type), `physical` from the record's `objects[]` |
| `additionalMetadata` | the release citation, the dataset citation and the record's own provenance |

Absent optional fields are omitted; a missing **required** field is a
[`check_eml()`](https://calcofi.io/calcofi4db/reference/check_eml.md)
finding, never a placeholder.

## See also

[`check_eml()`](https://calcofi.io/calcofi4db/reference/check_eml.md),
[`write_eml_files()`](https://calcofi.io/calcofi4db/reference/write_eml_files.md)
