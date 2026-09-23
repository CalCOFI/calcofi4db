# Mixed-layer depth of one CTD cast (threshold criterion)

The first depth below `ref_depth` where the profile departs from its
value at `ref_depth` by `threshold`, linearly interpolated between the
two bracketing samples (CalCOFI/workflows#101). The criterion is an
argument because the choice is Rasmus Swalethorp's to confirm; the
defaults are de Boyer Montegut et al. (2004, *JGR* 109, C12003):
reference 10 m, sigma-theta increase of 0.03 kg m^-3. The classic
alternative is 0.125 kg m^-3 (Levitus 1982; Monterey & Levitus 1997);
the temperature criterion (\|Delta T\| \>= 0.2 deg C) needs no salinity,
so it is the only one a `preliminary_without_bottle` cast can have.

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

  the departure that ends the mixed layer; default 0.03 for sigma-theta,
  0.2 for temperature.

- ref_depth:

  reference depth (m), default 10.

## Value

one-row tibble: `mld_m`, `ref_value`, `depth_max_m` (deepest good
sample), `status` (`ok`, `mixed_to_bottom`, `no_reference`, `no_data`),
`criterion`, `threshold`, `ref_depth`.

## Details

The value at `ref_depth` is interpolated from the samples either side of
it (it is `NA`, status `no_reference`, when the cast does not bracket
it); a cast that never crosses the threshold has `mld_m = NA` and status
`mixed_to_bottom` (its deepest sample is reported, so a consumer can say
"deeper than"). Flagged samples (8/9) are dropped first.

## Examples

``` r
z <- 0:100
ctd_mld(z, ifelse(z < 40, 25, 25.5))           # step at 40 m
#> # A tibble: 1 × 7
#>   mld_m ref_value depth_max_m status criterion   threshold ref_depth
#>   <dbl>     <dbl>       <dbl> <chr>  <chr>           <dbl>     <dbl>
#> 1  39.1        25         100 ok     sigma_theta      0.03        10
ctd_mld(z, ifelse(z < 40, 25, 25.5), threshold = 0.125)
#> # A tibble: 1 × 7
#>   mld_m ref_value depth_max_m status criterion   threshold ref_depth
#>   <dbl>     <dbl>       <dbl> <chr>  <chr>           <dbl>     <dbl>
#> 1  39.2        25         100 ok     sigma_theta     0.125        10
```
