# Seafloor depth at each sample position from a GEBCO GeoTIFF

Extracts, for every distinct position in `sample_tbl`, the bilinear
seafloor depth (positive down, land clamped to 0 — the same convention
as `calcofi4r::cc_bathy_depth()`) and the deepest cell in the 3x3
neighbourhood around it, which is what a plausibility check should
compare against: on a slope the neighbourhood is deeper than the cell,
and by the amount the slope warrants.

## Usage

``` r
sample_seafloor(
  con,
  gebco_tif,
  sample_tbl = "sample",
  key_col = "sample_key",
  lon_col = "longitude",
  lat_col = "latitude"
)
```

## Arguments

- con:

  DBI connection holding `sample_tbl`.

- gebco_tif:

  Path to a GEBCO GeoTIFF (elevation, metres, negative below sea level).
  Any extent works; positions outside it return NA.

- sample_tbl, key_col, lon_col, lat_col:

  Table and columns to read.

## Value

A data.frame: `<key_col>`, `seafloor_depth_m`, `seafloor_max3x3_m`.
