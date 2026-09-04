# The citation for a release of the integrated database

Decided wording (2026-09-03): *CalCOFI (YYYY). CalCOFI Integrated
Database, release vYYYY.MM.DD \[Data set\]. Scripps Institution of
Oceanography, NOAA Fisheries, and California Department of Fish and
Wildlife. https://doi.org/* — the db-schema URL for the version until
its Zenodo DOI exists. `all_versions = TRUE` gives the concept-DOI form
(no release in the title, `10.5281/zenodo.22281994`).

## Usage

``` r
release_citation(version, date = NULL, doi = NULL, all_versions = FALSE)
```

## Arguments

- version:

  `vYYYY.MM.DD`

- date:

  the release date (Date or `"YYYY-MM-DD"`); the year comes from the
  version when omitted

- doi:

  the version's DOI, when Zenodo has minted it

- all_versions:

  cite every version under the concept DOI

## Value

One string.
