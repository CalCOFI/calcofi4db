# Area crosswalk between two grids

Every pair of a previous and a current grid cell that overlap, with the
area of the overlap and its share of each cell. This is the only way to
carry a cell-keyed value from one grid to the other: when the grid was
rebuilt from the official station positions (CalCOFI/workflows#130), 84
of the 113 station cells kept a key whose polygon changed, so a key
matched by name can be a different piece of ocean. (The 112 previous
cells kept as they were are identity rows.)

## Usage

``` r
grid_crosswalk(prev, grid, key = "grid_key", crs_m = 3310, min_km2 = 1e-04)
```

## Arguments

- prev, grid:

  `sf` polygons in EPSG:4326, the previous and the current cells, each
  with the key column

- key:

  name of the key column in both (default `"grid_key"`)

- crs_m:

  equal-area CRS for the areas (default 3310)

- min_km2:

  an overlap smaller than this is a numerical sliver and is dropped
  (default 1e-4 km2, 100 square metres: cell vertices are rounded to
  1e-9 degrees, which along a 200 km edge shared by an unchanged cell
  and its neighbour is some 20 square metres)

## Value

a tibble: `prev_grid_key`, `grid_key`, `overlap_km2` (rounded to the
square metre), `prev_frac` (the overlap as a fraction of the previous
cell) and `grid_frac` (as a fraction of the current cell), both rounded
to 9 decimals; ordered by `prev_grid_key`, `grid_key`

## Details

Areas are planar in an equal-area projection (`crs_m`, default
California Albers), after the lon/lat edges of both grids are densified
so the projection keeps them where positions are keyed. The shares are
of each cell's **whole** area: `prev_frac` sums to less than one for a
previous cell part of which no current cell covers (land under a finer
coastline), and `grid_frac` for a current cell part of which no previous
cell covered. Nothing is rescaled to hide that;
[`check_grid_crosswalk()`](https://calcofi.io/calcofi4db/reference/check_grid_crosswalk.md)
reports it.

## Examples

``` r
if (FALSE) { # \dontrun{
xw <- grid_crosswalk(calcofi4r::cc_grid_v1, calcofi4r::cc_grid)
# where the previous cell st30-ln90 went, largest share first
xw[xw$prev_grid_key == "st30-ln90", ] |> dplyr::arrange(dplyr::desc(prev_frac))
} # }
```
