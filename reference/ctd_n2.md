# Buoyancy frequency squared (N^2) of one CTD cast, TEOS-10

Rasmus Swalethorp's request (email 2026-09-24, with his R draft "Boyancy
frequency and pycnocline.r" and the legacy `E_BuoyFreq_Calc.m`):
"Buoyancy frequency ... calculated just for the 1 m binned CTD data that
could be served up on the CTD transect plotter. This product tells us
something about the stability of the water column and is useful for
identifying layers of mixing", modernized "following TEOS-10 standards
using the Gibbs package". Absolute salinity (`gsw_SA_from_SP`) and
conservative temperature (`gsw_CT_from_t`), then `gsw_Nsquared()` on the
staggered mid-points, aligned back to the sample pressures by linear
interpolation (`rule = 2`, as the draft does), then a centred
triangular-weighted (Bartlett) running mean over `smooth_m` dbar.

## Usage

``` r
ctd_n2(
  temperature,
  salinity,
  pressure,
  longitude,
  latitude,
  q_temperature = NA,
  q_salinity = NA,
  smooth_m = 10
)
```

## Arguments

- temperature:

  in-situ temperature (deg C, ITS-90), one cast (a scalar is recycled).

- salinity:

  practical salinity (a scalar is recycled).

- pressure:

  sea pressure (dbar).

- longitude, latitude:

  the cast's position (scalar or per sample).

- q_temperature, q_salinity:

  quality codes (`NA` = good).

- smooth_m:

  running-mean window (dbar), default 10.

## Value

numeric vector, N^2 (s^-2), the length of the input.

## Details

The smoothing is the one departure from the draft, which fits
`smooth.spline(spar = 0.4)` to the N^2 profile before searching for the
pycnocline: a weighted mean over a stated window gives the same answer
on every machine with no tuning parameter, and the legacy bottle script
(`F_PycnoclineDepth.m`) used a 10 m centred difference of the
interpolated density, which is the same window. The weights fall
linearly from the centre to the window's edge, so a one-bin step smooths
to a peak at the step (a flat box mean would make a plateau whose
shallow edge wins the maximum). As in that script, the value is
undefined (`NA`) within half a window of the profile's top and bottom,
where a truncated window would inflate a surface step. `smooth_m = 0`
returns the aligned raw N^2 at every sample. A sample whose temperature
or salinity is flagged 8/9 or missing takes no part in the computation
and is `NA` in the result; the output is aligned to the input.

## Examples

``` r
p <- 1:100
n2 <- ctd_n2(ifelse(p < 50, 16, 10), 33.5, p, -120, 33)
p[which.max(n2)]   # near the 50 dbar interface
#> [1] 49
```
