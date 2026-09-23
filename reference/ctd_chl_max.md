# Depth of the chlorophyll-a maximum of one CTD cast

The depth of the maximum of a running median of the profile (default 5 m
window), so a single spiking bin cannot be "the max"
(CalCOFI/workflows#102). Use the bottle-fitted sensor estimate
`est_chlorophyll_a_sta_corr` (Rasmus Swalethorp, 2026-09-09). The window
is in samples of the 1 m bins (`window_m` bins, forced odd); on
irregular spacing it is a window of that many samples. Flagged samples
are dropped first. Where the median flattens the peak into a plateau,
the plateau depth with the highest raw value wins (then the shallowest).

## Usage

``` r
ctd_chl_max(depth, chl, qual = NA, window_m = 5)
```

## Arguments

- depth:

  depth (m), one cast.

- chl:

  chlorophyll-a (mg m^-3).

- qual:

  quality codes of `chl` (`NA` = good).

- window_m:

  running-median window (1 m bins), default 5.

## Value

one-row tibble: `chl_max_depth_m`, `chl_max_value` (the smoothed value
there), `n`.

## Examples

``` r
z <- 0:150
ctd_chl_max(z, 0.2 + 2 * exp(-((z - 40) / 10)^2))
#> # A tibble: 1 × 3
#>   chl_max_depth_m chl_max_value     n
#>             <dbl>         <dbl> <int>
#> 1              40          2.18   151
```
