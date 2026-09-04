# Write the release citation into a `catalog.json` list

Sets `citation` (from
[`release_citation()`](https://calcofi.io/calcofi4db/reference/release_citation.md))
and `concept_doi`; `doi` is set when given and kept when the catalog
already has one, and the citation uses it. Everything else in the
catalog is untouched.

## Usage

``` r
add_release_citation(catalog, doi = NULL, concept_doi = CC_ZENODO_CONCEPT_DOI)
```

## Arguments

- catalog:

  the parsed catalog (list)

- doi:

  the version DOI, once Zenodo has minted it

- concept_doi:

  the concept DOI (all versions)

## Value

The catalog.
