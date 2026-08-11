# Convert CalCOFI line/station to longitude/latitude

The CalCOFI station plan is a coordinate system in its own right, and
PROJ ships it as `+proj=calcofi` — so this is a projection, not a lookup
against `grid`. That distinction matters: a lookup only resolves
stations that exist in the grid table, while the transform resolves
**any** line/station pair, including the historical inshore stations and
the Gulf of California and Baja lines that the modern pattern dropped.

## Usage

``` r
cc_calcofi_to_lonlat(line, station)
```

## Arguments

- line, station:

  numeric vectors of CalCOFI line and station, recycled to a common
  length. `NA` in either yields `NA` in both outputs.

## Value

a data.frame with `longitude` and `latitude` (WGS 84), one row per input

## Details

Use it to recover a position for a row that records where it was in
CalCOFI terms but carries no lon/lat. Once a position exists, `hex_id`
and `grid_key` follow from it in the usual way (`.hex_expr()`,
[`assign_grid_key()`](https://calcofi.io/calcofi4db/reference/assign_grid_key.md)),
so a recovered row becomes a full participant in spatial rollups rather
than an ungridded remainder.

## Examples

``` r
if (FALSE) { # \dontrun{
cc_calcofi_to_lonlat(90, 60)   # -119.96, 32.42 — off San Diego
} # }
```
