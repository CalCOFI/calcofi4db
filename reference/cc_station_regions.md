# Region polygons from a station-membership list

Some datasets pool their samples across a named set of CalCOFI stations
before measuring — the counting happened at the microscope, so there is
no per-station observation and no `grid_key` to hang one on. All the
source gives is *which stations went into which region*. This turns that
membership list into one polygon per region.

## Usage

``` r
cc_station_regions(x, group = "region", line = "line", station = "station")
```

## Arguments

- x:

  data.frame of station membership, one row per (region, station).

- group, line, station:

  column names in `x` holding the region label and the CalCOFI line and
  station. Defaults `"region"`, `"line"`, `"station"`.

## Value

an `sf` with one row per region: the `group` column, `n_stations`,
`longitude`/`latitude` of a representative point **guaranteed to fall
inside the region's own polygon** (regions are concave — one wrapping
another puts a centroid outside it), and `geom`, a `POLYGON` in
EPSG:4326.

## Details

The naive reading — a convex hull over each region's member stations —
fails on real membership lists in three ways, all of them silent:

- **A region whose stations are collinear has no hull.** Four stations
  on one CalCOFI line give a zero-width slab, not a region.

- **Regions interleave, so their hulls overlap.** A hull claims
  everything between its members, including the parts another region
  owns.

- **The hulls do not tile.** Space between regions belongs to nobody, so
  a point-in-polygon lookup returns nothing for a third of the sampled
  domain.

So the partition is built the other way round: every station claims the
area nearest to it (a Voronoi tessellation), the cells are clipped to
the convex hull of *all* the stations, and then dissolved by region. The
result tiles the pooled domain exactly — no overlaps, no gaps — and each
region comes out as one connected piece even when its own members are
not adjacent, which a union of member cells cannot do.

The outer boundary is the hull of the stations themselves, deliberately:
the pooling says nothing about water beyond the outermost station
occupied, and padding it outward would be inventing extent. Land is
**not** erased — the geometry describes where the sampling was, and
subtracting a coastline would bind the released polygons to one
coastline vintage. Erase at render time if a map needs it.

Positions come from
[`cc_calcofi_to_lonlat()`](https://calcofi.io/calcofi4db/reference/cc_calcofi_to_lonlat.md)
rather than a `grid` lookup, so a historical inshore station outside the
modern pattern places exactly like any other instead of dropping out of
its region.

## Examples

``` r
if (FALSE) { # \dontrun{
# the four Venrick phytoplankton pooling regions
cc_station_regions(data.frame(
  region  = c("SE", "SE",   "Offshore", "Offshore"),
  line    = c(93.3, 93.3,   93.3,       93.3),
  station = c(30,   40,     70,         80)))
} # }
```
