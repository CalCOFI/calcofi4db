# Mixed-layer depth of one CTD cast (threshold criterion)

The first depth below `ref_depth` where the profile departs from its
value at `ref_depth` by `threshold`, linearly interpolated between the
two bracketing samples (CalCOFI/workflows#101). The default is the
provider's definition, the CalCOFI legacy one (Rasmus Swalethorp,
2026-09-23, questions.csv Q01, adopted 2026-10-01): "MLD is the depth at
which sigma-theta is 0.02 kg/m3 greater than at a reference depth of 10
m". The criterion stays an argument: the alternatives published beside
it are the classic 0.125 kg m^-3 (Levitus 1982; Monterey & Levitus 1997)
and the temperature criterion (\|Delta T\| \>= 0.2 deg C, de Boyer
Montegut et al. 2004), which needs no salinity, so it is the only one a
`preliminary_without_bottle` cast can have. The 0.03 kg m^-3 default of
calcofi4db 4.16.0-4.17.x (de Boyer Montegut et al. 2004) is superseded
and only reachable as `threshold = 0.03`.

## Usage

``` r
ctd_mld(
  depth,
  value,
  qual = NA,
  criterion = c("sigma_theta", "temperature"),
  threshold = NULL,
  ref_depth = 10
)
```

## Arguments

- depth:

  depth (m), positive down; one cast.

- value:

  sigma-theta (kg m^-3) or temperature (deg C), per `criterion`.

- qual:

  quality codes of `value` (`NA` = good).

- criterion:

  `"sigma_theta"` (value must increase by `threshold`) or
  `"temperature"` (absolute departure of `threshold`).

- threshold:

  the departure that ends the mixed layer; default 0.02 for sigma-theta
  (the provider's definition), 0.2 for temperature.

- ref_depth:

  reference depth (m), default 10.

## Value

one-row tibble: `mld_m`, `ref_value`, `depth_max_m` (deepest good
sample), `status` (`ok`, `mixed_to_bottom`, `no_reference`, `no_data`),
`criterion`, `threshold`, `ref_depth`.

## Details

The provider's answer is silent on the edge cases, so these are
unchanged from 4.16.0: the value at `ref_depth` is interpolated from the
samples either side of it (it is `NA`, status `no_reference`, when the
cast does not bracket it, e.g. a cast starting below 10 m); a cast that
never crosses the threshold has `mld_m = NA` and status
`mixed_to_bottom` (its deepest sample is reported, so a consumer can say
"deeper than"). Flagged (8/9), missing and non-finite samples are
dropped first, so a `NaN` bin is a gap the interpolation spans.

## Examples

``` r
z <- 0:100
ctd_mld(z, ifelse(z < 40, 25, 25.5))           # step at 40 m, the provider's 0.02
#> # A tibble: 1 × 7
#>   mld_m ref_value depth_max_m status criterion   threshold ref_depth
#>   <dbl>     <dbl>       <dbl> <chr>  <chr>           <dbl>     <dbl>
#> 1  39.0        25         100 ok     sigma_theta      0.02        10
ctd_mld(z, ifelse(z < 40, 25, 25.5), threshold = 0.125)
#> # A tibble: 1 × 7
#>   mld_m ref_value depth_max_m status criterion   threshold ref_depth
#>   <dbl>     <dbl>       <dbl> <chr>  <chr>           <dbl>     <dbl>
#> 1  39.2        25         100 ok     sigma_theta     0.125        10
```
