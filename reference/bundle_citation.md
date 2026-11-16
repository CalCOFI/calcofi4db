# The citation a dataset's bundle carries

Cite both: the dataset (its provider's `citation_main`, with its DOI and
any `citation_others`) and the CalCOFI release it was downloaded from
(`catalog.json`'s `citation`, with the version DOI and the concept DOI
for all versions). Where the provider gave no citation, `CITATION.md`
says so and no dataset entry is invented: the `.bib` carries a comment
instead. The BibTeX entries take calcofi4r::cc_cite()'s shape.

## Usage

``` r
bundle_citation(record, release)
```

## Arguments

- record:

  one dataset record from `datasets.json`

- release:

  a list: `version`, `release_date`, `citation`, `doi`, `concept_doi`
  (from the release's `catalog.json`)

## Value

A list: `md` (the `CITATION.md` text), `bib` (the `CITATION.bib` text)
and `has_provider_citation` (logical).
