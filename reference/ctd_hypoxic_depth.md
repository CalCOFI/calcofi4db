# Hypoxic boundary depths of one CTD cast

Rasmus Swalethorp's definition (email 2026-09-24): "Same as nitracline
but here the thresholds should be set at 2.4 mL/L O2 for mild hypoxic
boundary, 1.4 for hypoxic boundary and 0.5 for severe hypoxic boundary
(core oxygen minimum zone, OMZ). The OMZ sometimes sits in the middle of
the CTD profile meaning you can encounter the boundaries both above and
below the OMZ. I suggest we just focus on the shallowest boundary
depths." Each threshold is
[`profile_threshold_depth()`](https://calcofi.io/calcofi4db/reference/profile_threshold_depth.md)
with `direction = "below"`: the shallowest depth where oxygen falls to
the threshold, interpolated linearly between the bracketing bins. Use
the bottle-corrected sensor-pair average in mL/L
(`oxygen_ml_l_ave_sta_corr`).

## Usage

``` r
ctd_hypoxic_depth(
  depth,
  oxygen,
  qual = NA,
  thresholds = c(hypoxic_depth_24 = 2.4, hypoxic_depth_14 = 1.4, hypoxic_depth_05 = 0.5),
  surface_max_m = 20
)
```

## Arguments

- depth:

  depth (m), one cast.

- oxygen:

  dissolved oxygen (mL/L).

- qual:

  quality codes of `oxygen` (`NA` = good).

- thresholds:

  named numeric: the measurement type key of each boundary and its
  oxygen threshold (mL/L); the defaults are the registry keys.

- surface_max_m:

  passed to
  [`profile_threshold_depth()`](https://calcofi.io/calcofi4db/reference/profile_threshold_depth.md).

## Value

a tibble with one row per threshold, in the order given:
`measurement_type` plus the columns of
[`profile_threshold_depth()`](https://calcofi.io/calcofi4db/reference/profile_threshold_depth.md).

## Examples

``` r
z <- seq(0, 500, by = 10)
ctd_hypoxic_depth(z, 6 - z / 100)
#> # A tibble: 3 × 9
#>   measurement_type depth_m value_above value_below status    threshold direction
#>   <chr>              <dbl>       <dbl>       <dbl> <chr>         <dbl> <chr>    
#> 1 hypoxic_depth_24     360         2.5         2.4 ok              2.4 below    
#> 2 hypoxic_depth_14     460         1.5         1.4 ok              1.4 below    
#> 3 hypoxic_depth_05      NA        NA          NA   not_reac…       0.5 below    
#> # ℹ 2 more variables: depth_max_m <dbl>, n <int>
```
