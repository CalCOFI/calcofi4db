# The README of a dataset's bundle

Plain language: what the dataset is and who collected it, the release
version and date, what each file holds and how many rows, how the tables
join, what was left out and where to get it, how to take a subset by
place and time in the Explorer, the dataset's page, and the
register-your-use and stay-informed links.

## Usage

``` r
bundle_readme(
  record,
  release,
  format,
  tables,
  plan,
  decision,
  license,
  has_citation = TRUE,
  meta_tables = list()
)
```

## Arguments

- record:

  one dataset record from `datasets.json`

- release:

  a list with `version` and `release_date` (the release the bundle was
  built from)

- format:

  one of
  [`bundle_formats()`](https://calcofi.io/calcofi4db/reference/bundle_formats.md)

- tables:

  the data frame from
  [`bundle_build_tables()`](https://calcofi.io/calcofi4db/reference/bundle_build_tables.md)

- plan:

  the data frame from
  [`bundle_plan()`](https://calcofi.io/calcofi4db/reference/bundle_plan.md)
  (for the exclusions)

- decision:

  the data frame from
  [`bundle_format_decision()`](https://calcofi.io/calcofi4db/reference/bundle_format_decision.md)

- license:

  the list from
  [`bundle_license()`](https://calcofi.io/calcofi4db/reference/bundle_license.md)

- has_citation:

  logical, from
  [`bundle_citation()`](https://calcofi.io/calcofi4db/reference/bundle_citation.md)

- meta_tables:

  `metadata.json`'s `tables` block, for a table this package has no
  label for

## Value

Character, the Markdown text.
