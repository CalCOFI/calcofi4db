# One dataset's entry in the bundle manifest the website reads

The contract between `publish_to-zip.qmd` and calcofi.io: per dataset,
the release its content first appeared in (`since`, also `version` for a
schema-1.0 reader) and the release this run confirmed it against
(`checked_version`), its identities — `content_hash` (data + its
metadata record + the pinned format; names the object's directory, as a
release table's content_hash does) and `data_hash` (the data alone) —
the licence kind, whether the provider gave a citation, the rows of
every table, what was left out, and per format either the file (`path`,
`url`, `bytes`, `sha256`, `excel_openable`) or the reason it is not
offered, in plain words. Each release's manifest is kept
(`publish/zip/releases/{version}/manifest.json`), so the history of a
dataset's packages is the chain of manifests, as a release table's is
the chain of catalogs; an object a newer content replaced stays at its
path.

## Usage

``` r
bundle_manifest_entry(
  record,
  version,
  release_date,
  checked_version,
  fingerprint,
  tables,
  plan,
  written,
  license_kind,
  has_provider_citation,
  base_url = CC_BUNDLE_BASE_URL
)
```

## Arguments

- record:

  one dataset record from `datasets.json`

- version:

  the release the bundles' content first appeared in (`since`)

- release_date:

  that release's date

- checked_version:

  the release this run confirmed them against

- fingerprint:

  the list from
  [`publish_fingerprint()`](https://calcofi.io/calcofi4db/reference/publish_fingerprint.md)

- tables:

  the data frame from
  [`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)

- plan:

  the data frame from
  [`bundle_plan()`](https://calcofi.io/calcofi4db/reference/bundle_plan.md)

- written:

  the data frame from
  [`write_bundle()`](https://calcofi.io/calcofi4db/reference/write_bundle.md)

- license_kind, has_provider_citation:

  from
  [`bundle_license()`](https://calcofi.io/calcofi4db/reference/bundle_license.md)
  /
  [`bundle_citation()`](https://calcofi.io/calcofi4db/reference/bundle_citation.md)

- base_url:

  the public root the zips are served from
  (`{base_url}/{dataset_key}/{content_hash[1:24]}/{file}`,
  [`publish_object_path()`](https://calcofi.io/calcofi4db/reference/publish_object_path.md))

## Value

A named list, ready for `jsonlite::toJSON(auto_unbox = TRUE)`.
