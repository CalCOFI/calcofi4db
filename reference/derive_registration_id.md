# The identifier a portal knows a dataset by, read off its own URL

A fallback, never an override: a curated `id` in
`metadata/distribution.csv` always wins. Returns `NA_character_` when
the URL says nothing — a portal home, a search page — because a guessed
identifier is worse than none.

## Usage

``` r
derive_registration_id(url)
```

## Arguments

- url:

  character vector of URLs

## Value

character vector of the same length

## Details

Recognised: EDI (`packageid=`, or `scope=&identifier=&revision=`), NCEI
(`id=`), OBIS (`/dataset/{uuid}`), a GBIF/OBIS IPT resource (`?r=`), any
DOI (`doi.org/10.…`), CalOOS (`#module-metadata/{uuid}`), any ERDDAP (a
`tabledap`/`griddap` page, an `info` page, or an ISO 19115 / FGDC
document), DataZoo (`/datasets/{n}`) and an NCBI BioProject.

## Examples

``` r
derive_registration_id("https://obis.org/dataset/0e223f55-c826-4513-ae9a-b04cbf2e189c")
#> [1] "0e223f55-c826-4513-ae9a-b04cbf2e189c"
derive_registration_id("https://portal.edirepository.org/nis/mapbrowse?packageid=edi.109.4")
#> [1] "edi.109.4"
```
