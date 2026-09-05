# Which portal family a URL belongs to

Host-based: `edi` (edirepository.org, pasta.lternet.edu), `ncei`,
`erddap-noaa` (coastwatch / oceanview / upwell `pfeg.noaa.gov` ERDDAPs),
`erddap-calcofi`, `datazoo` and the other oceaninformatics.ucsd.edu
portals (ZooDB, ZooScan), `ucsd-library`, `obis`, `ipt`, `caloos`,
`zenodo`, `ncbi`, `calcofi.org`, `gcs` (storage.googleapis.com /
storage.calcofi.io), else `other`. `NA` for an empty input.

## Usage

``` r
classify_portal(url)
```

## Arguments

- url:

  character vector

## Value

character vector of the same length, values from
[`distribution_portals()`](https://calcofi.io/calcofi4db/reference/distribution_kinds.md).
