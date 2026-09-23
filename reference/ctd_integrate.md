# Depth-integrate one CTD cast's profile

Trapezoidal integral of `value` from the surface to `z_max` (default 200
m) or to the deepest good sample if shallower, which is recorded
(CalCOFI/workflows#102; Rasmus Swalethorp, 2026-09-16: integrated
chlorophyll "is just summing up all the 1 m bins" — on 1 m bins the
trapezoid and the sum differ only by half the two end bins). The
shallowest good sample is carried up to the surface when it is no deeper
than `z_top_max` (default 5 m); a cast that starts deeper is `NA`
(status `no_surface`), so a missing surface is never silently skipped.
Flagged samples are dropped first. For chlorophyll-a in mg m^-3 the
result is mg m^-2.

## Usage

``` r
ctd_integrate(depth, value, qual = NA, z_max = 200, z_top_max = 5)
```

## Arguments

- depth:

  depth (m), one cast.

- value:

  the quantity per unit volume.

- qual:

  quality codes of `value` (`NA` = good).

- z_max:

  integration floor (m), default 200.

- z_top_max:

  deepest acceptable first sample (m), default 5.

## Value

one-row tibble: `integrated`, `depth_reached_m`, `status` (`ok`,
`shallow`, `no_surface`, `no_data`), `z_max`.

## Examples

``` r
ctd_integrate(0:300, rep(1, 301))   # 200 over 0-200 m
#> # A tibble: 1 × 4
#>   integrated depth_reached_m status z_max
#>        <dbl>           <dbl> <chr>  <dbl>
#> 1        200             200 ok       200
```
