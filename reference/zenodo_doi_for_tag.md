# Find the Zenodo record (and DOI) minted for a release tag

Zenodo's GitHub integration archives this repository at each release tag
and records `https://github.com/{repo}/tree/{tag}` as a related
identifier. `zenodo_doi_for_tag()` searches for that identifier, then
falls back to the concept's version listing matched on
`metadata.version`. `NULL` when no record exists yet (the DOI arrives
minutes after the GitHub release).

## Usage

``` r
zenodo_doi_for_tag(
  tag,
  repo = "CalCOFI/workflows",
  concept_doi = CC_ZENODO_CONCEPT_DOI,
  fetch = NULL
)

zenodo_record_for_tag(json, tag, repo = "CalCOFI/workflows")
```

## Arguments

- tag:

  the release tag, e.g. `"v2026.09.03"`

- repo:

  the GitHub repository

- concept_doi:

  the concept DOI whose versions are listed as a fallback

- fetch:

  the HTTP function (see
  [`check_dataset_citation()`](https://calcofi.io/calcofi4db/reference/check_dataset_citation.md))

- json:

  a Zenodo records search response (text)

## Value

A list — `doi`, `concept_doi`, `record_id`, `version`, `url`, `title` —
or `NULL`.
