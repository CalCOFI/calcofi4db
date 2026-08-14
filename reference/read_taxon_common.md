# Read the vernacular-name registry

Strict read of `metadata/taxon_common.csv` — every column character, so
an empty cell stays empty rather than round-tripping as the string
`"NA"`.

## Usage

``` r
read_taxon_common(path)
```

## Arguments

- path:

  path to the registry; a missing file yields an empty frame, so a first
  run and a deleted cache behave alike.

## Value

a data frame with the registry columns.
