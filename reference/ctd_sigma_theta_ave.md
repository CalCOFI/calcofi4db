# Averaged sigma-theta from a CTD sensor pair

`sigma_theta_1` / `sigma_theta_2` (the files' `SigThetaTS1` /
`SigThetaTS2`) combined by the same flag rule as every other CTD pair —
see
[`combine_sensor_pair()`](https://calcofi.io/calcofi4db/reference/combine_sensor_pair.md):
a sensor flagged 8/9 is dropped, 1/2 select a sensor, otherwise the mean
(Rasmus Swalethorp, 2026-09-22: "average unless one is flagged";
CalCOFI/workflows#99).

## Usage

``` r
ctd_sigma_theta_ave(s1, s2, q1 = NA, q2 = NA)
```

## Arguments

- s1, s2:

  numeric: sigma-theta from sensor pair 1 and 2 (kg m^-3).

- q1, q2:

  their quality codes (`NA` = good).

## Value

numeric vector, the averaged sigma-theta.

## Examples

``` r
ctd_sigma_theta_ave(c(25.1, 25.1), c(25.3, 25.3), q1 = c(NA, "9"))
#> [1] 25.2 25.3
# 25.2 25.3
```
