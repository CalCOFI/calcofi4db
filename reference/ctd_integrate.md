# Depth-integrate one CTD cast's profile

The provider's definition for the CTD (Rasmus Swalethorp, 2026-09-23,
questions.csv Q02, adopted 2026-10-01): "For the CTD data here we should
just sum up all the 1m bins within the top 200m or as deep as the
station was on shallower stations. For the bottle data we will need to
integrate between points. I believe trapezoidal integration was used in
the past" (CalCOFI/workflows#102). So `method = "sum"` (the default)
sums the `bin_m` bins from the surface to `z_max` (default 200 m), or to
the deepest good sample if shallower, which is recorded;
`method = "trapezoid"` is the trapezoidal integral between points, for
bottle data (and the CTD rule of calcofi4db 4.16.0-4.17.x). For
chlorophyll-a in mg m^-3 the result is mg m^-2.

## Usage

``` r
ctd_integrate(
  depth,
  value,
  qual = NA,
  z_max = 200,
  z_top_max = 5,
  method = c("sum", "trapezoid"),
  bin_m = 1
)
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

- method:

  `"sum"` (the provider's CTD rule: the sum of the `bin_m` bins) or
  `"trapezoid"` (between points, for bottle data).

- bin_m:

  the bin thickness `"sum"` adds up (m), default 1.

## Value

one-row tibble: `integrated`, `depth_reached_m`, `status` (`ok`,
`shallow`, `no_surface`, `no_data`), `z_max`, `method`.

## Details

The sum runs over the bins centred at `bin_m, 2 * bin_m, ..., bottom`
(200 bins of 1 m in the top 200 m). The answer is silent on missing
bins, so the 4.16.0 behaviour is kept for both methods: the shallowest
good sample is carried up to the surface when it is no deeper than
`z_top_max` (default 5 m), a cast that starts deeper is `NA` (status
`no_surface`, so a missing surface is never silently skipped), and a bin
missing inside the profile (a flagged or `NaN` bin) is filled by linear
interpolation between its neighbours rather than counted as zero.
Flagged (8/9), missing and non-finite samples are dropped first.

## Examples

``` r
ctd_integrate(0:300, rep(1, 301))                       # 200 over 0-200 m
#> # A tibble: 1 × 5
#>   integrated depth_reached_m status z_max method
#>        <dbl>           <dbl> <chr>  <dbl> <chr> 
#> 1        200             200 ok       200 sum   
ctd_integrate(c(0, 100, 300), c(1, 1, 3), method = "trapezoid")
#> # A tibble: 1 × 5
#>   integrated depth_reached_m status z_max method   
#>        <dbl>           <dbl> <chr>  <dbl> <chr>    
#> 1        250             200 ok       200 trapezoid
```
