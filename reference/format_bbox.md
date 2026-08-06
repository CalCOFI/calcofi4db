# Format a Bounding Box as a Human-Readable Extent

Renders a decimal-degree bounding box the way a data catalog writes one:
unsigned magnitudes carrying a hemisphere suffix, in geographic order
(south to north, west to east).

## Usage

``` r
format_bbox(lat_min, lat_max, lon_min, lon_max, digits = 1)
```

## Arguments

- lat_min, lat_max, lon_min, lon_max:

  Bounds in decimal degrees. Any non-finite value (`NA`, `NaN`, `±Inf`)
  yields `NA_character_` — a partial box is not a box.

- digits:

  Decimal places to show.

## Value

Length-1 character, e.g. `"29.8–37.8°N, 126.5–117.3°W"`, or `NA`.

## Details

Geographic order is preserved rather than numeric order, so a western
longitude span reads `"126.5–117.3°W"` — west edge first — instead of
the signed `"-126.5 to -117.3"`. When a span crosses the equator or the
prime meridian the two ends carry their own suffix (`"3.2°S–12.7°N"`),
because a single trailing hemisphere would silently mislabel half the
range.

## Examples

``` r
format_bbox(29.8, 37.8, -126.5, -117.3)
#> [1] "29.8–37.8°N, 126.5–117.3°W"
```
