# Spice (spiciness at 0 dbar, TEOS-10) per CTD sample

Rasmus Swalethorp's recipe (email 2026-09-22; CalCOFI/workflows#100),
with the Gibbs SeaWater toolbox: absolute salinity from practical
salinity, conservative temperature from in-situ temperature, then
`gsw_spiciness0()` (McDougall & Krzysik 2015, *J. Mar. Res.* 73,
141-152). Positive is "spicy" (warm, salty), negative "minty" (cool,
fresh). Pass the sensor-pair averaged, corrected series
(`temperature_ave`, `salinity_ave_corr`), not a single sensor; a sample
whose temperature or salinity is flagged 8/9 is `NA`.

## Usage

``` r
ctd_spice(
  temperature,
  salinity,
  pressure,
  longitude,
  latitude,
  q_temperature = NA,
  q_salinity = NA
)
```

## Arguments

- temperature:

  in-situ temperature (deg C, ITS-90).

- salinity:

  practical salinity (PSS-78).

- pressure:

  sea pressure (dbar); depth in metres is an acceptable stand-in in the
  upper 500 m (the difference is \< 1 %, well inside spice's
  sensitivity).

- longitude, latitude:

  decimal degrees (recycled).

- q_temperature, q_salinity:

  quality codes (`NA` = good).

## Value

numeric vector, spiciness (kg m^-3).

## Examples

``` r
ctd_spice(15, 33.5, 10, -120, 33)
#> [1] 1.173565
```
