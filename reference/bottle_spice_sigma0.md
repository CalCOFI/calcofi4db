# Spice and TEOS-10 sigma-theta per bottle

For each bottle, from its own in-situ temperature and practical salinity
at its depth: sea pressure from depth and latitude
(`gsw_p_from_z(-depth, latitude)`), absolute salinity
(`gsw_SA_from_SP`), conservative temperature (`gsw_CT_from_t`), then

- `spiciness0`, spiciness at 0 dbar (`gsw_spiciness0`; McDougall &
  Krzysik 2015), by
  [`ctd_spice()`](https://calcofi.io/calcofi4db/reference/ctd_spice.md),
  the same rule as the CTD series;

- `sigma_theta_teos10`, potential density anomaly referenced to 0 dbar
  (`gsw_sigma0`), the TEOS-10 sigma-theta.

## Usage

``` r
bottle_spice_sigma0(
  temperature,
  salinity,
  depth,
  longitude,
  latitude,
  q_temperature = NA,
  q_salinity = NA
)
```

## Arguments

- temperature:

  in-situ temperature (deg C).

- salinity:

  practical salinity (PSS-78).

- depth:

  depth of the bottle (m, positive down).

- longitude, latitude:

  decimal degrees (recycled).

- q_temperature, q_salinity:

  quality codes of `temperature` and `salinity` (`NA` = good).

## Value

a tibble with one row per input: `pressure_dbar`, `spiciness0` (kg
m^-3), `sigma_theta_teos10` (kg m^-3). `pressure_dbar` is `NA` where
either value is.

## Details

**Flags first.** A bottle whose temperature or salinity its provider
flags 8 (suspect) or 9 (missing) gets neither value: a derived value
carries no flag, so nothing downstream could undo a flagged input. Code
6 ("data OK but taken from CTD") and an empty code are good. Codes are
normalized as
[`combine_sensor_pair()`](https://calcofi.io/calcofi4db/reference/combine_sensor_pair.md)
does (`"8.0"` is `"8"`). A missing or non-finite input gives `NA`, never
an error.

## References

McDougall, T. J. and O. A. Krzysik (2015), Spiciness, *J. Mar. Res.* 73,
141-152, doi:10.1357/002224015816665589. IOC, SCOR and IAPSO (2010),
*The international thermodynamic equation of seawater - 2010 (TEOS-10)*,
IOC Manuals and Guides 56.

## Examples

``` r
bottle_spice_sigma0(c(15, 15, 15), c(33.5, 33.5, 33.5), 10, -120, 33,
                    q_salinity = c(NA, "6", "8"))
#> # A tibble: 3 × 3
#>   pressure_dbar spiciness0 sigma_theta_teos10
#>           <dbl>      <dbl>              <dbl>
#> 1          10.1       1.17               24.8
#> 2          10.1       1.17               24.8
#> 3          NA        NA                  NA  
```
