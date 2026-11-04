# Pycnocline depth and maximum N^2 of one CTD cast

Rasmus Swalethorp's definition (email 2026-09-24, R draft "Boyancy
frequency and pycnocline.r"; the legacy bottle script
`F_PycnoclineDepth.m` after Fiedler et al. 2013, *J. Climate*): "The
pycnocline is defined physically as the depth of maximum static
stability": the pressure at which the smoothed N^2 of
[`ctd_n2()`](https://calcofi.io/calcofi4db/reference/ctd_n2.md) is
greatest within the search window `p_min`-`p_max` dbar (the draft's
10-200 m), and `n2_max` that value. The legacy script's conditions are
kept: the cast must start no deeper than `surface_max_m` (15 m) and
reach at least `depth_min_m` (50 m), with at least `min_points` (5) good
values inside the window; an exact tie goes to the centre of the tied
run.

## Usage

``` r
ctd_pycnocline(
  pressure,
  n2,
  p_min = 10,
  p_max = 200,
  min_points = 5,
  surface_max_m = 15,
  depth_min_m = 50
)
```

## Arguments

- pressure:

  sea pressure (dbar), one cast.

- n2:

  the smoothed N^2 at those pressures, from
  [`ctd_n2()`](https://calcofi.io/calcofi4db/reference/ctd_n2.md) (`NA`
  where flagged).

- p_min, p_max:

  the search window (dbar), default 10-200.

- min_points:

  fewest good values inside the window, default 5.

- surface_max_m:

  deepest acceptable first good sample (m), default 15.

- depth_min_m:

  shallowest acceptable deepest good sample (m), default 50.

## Value

one-row tibble: `pycnocline_depth_m`, `n2_max`, `status` (`ok`,
`no_surface`, `too_shallow`, `too_few`, `no_data`), `n` (good values
inside the window).

## Examples

``` r
p <- 1:300
n2 <- ctd_n2(ifelse(p < 60, 16, 10), 33.5, p, -120, 33)
ctd_pycnocline(p, n2)   # about 60 dbar
#> # A tibble: 1 × 4
#>   pycnocline_depth_m  n2_max status     n
#>                <dbl>   <dbl> <chr>  <int>
#> 1                 59 0.00206 ok       191
```
