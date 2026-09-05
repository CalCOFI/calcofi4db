# The findings [`check_eml()`](https://calcofi.io/calcofi4db/reference/check_eml.md) can report, with their level

`error` findings fail the release unless exempt (an open/proposed
`questions.csv` row on `related_table = dataset` naming the field, or
naming none, covers it — the same rule
[`check_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/check_dataset_catalog.md)
applies to `no_citation`); `warn` findings never block but are always
printed, because each one names a real gap in the record.

## Usage

``` r
eml_findings()
```

## Value

A named character vector, finding -\> level.

## Details

- `invalid_eml` —
  [`EML::eml_validate()`](https://docs.ropensci.org/emld/reference/eml_validate.html)
  rejected the written document.

- `no_title`, `no_abstract`, `no_pub_date`, `no_geographic_coverage`,
  `no_temporal_coverage`, `no_data_table` — an element EDI's EML
  checklist requires that the record could not supply.

- `no_creator` — no `creators[]`, no `pi_names` and no registered
  provider.

- `no_license` — no `license` on the record (exempt while a licence
  question is open).

- `short_abstract` — under 20 words (EDI's guidance; the record's own
  text, not a stub).

- `creator_from_provider` — the creator is the provider organization,
  because the record names no person.

- `contact_role_address` — the contact is
  [`eml_contact_address()`](https://calcofi.io/calcofi4db/reference/eml_contact_address.md),
  because no provider address is on record.

- `creator_no_organization`, `no_keywords`, `no_methods`,
  `no_taxonomic_coverage`, `undocumented_attributes`, `custom_units` — a
  gap that weakens the document without invalidating it.
