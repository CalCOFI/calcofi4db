# The netCDF `manifests.json` of every dataset published by `publish_to-netcdf.qmd`

The netCDF `manifests.json` of every dataset published by
`publish_to-netcdf.qmd`

## Usage

``` r
fetch_netcdf_manifests(keys, base = CC_NETCDF_HTTPS, fetch = NULL)
```

## Arguments

- keys:

  dataset keys (and any `{key}_full` variants) to look up

- base:

  the HTTPS root of `netcdf/`

- fetch:

  the HTTP function

## Value

A named list, one parsed `manifests.json` per key that answered.
