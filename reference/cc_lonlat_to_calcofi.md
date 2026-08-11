# Convert longitude/latitude to CalCOFI line/station

The inverse of
[`cc_calcofi_to_lonlat()`](https://calcofi.io/calcofi4db/reference/cc_calcofi_to_lonlat.md).
Returns the CONTINUOUS line/station position, not the nearest standard
station — 90.7 is a real answer, not a rounding error, and rounding it
would silently move a sample onto a station it was not taken at. Round
deliberately at the call site if a station label is what you want.

## Usage

``` r
cc_lonlat_to_calcofi(lon, lat)
```

## Arguments

- lon, lat:

  numeric vectors (WGS 84), recycled to a common length

## Value

a data.frame with `line` and `station`, one row per input
