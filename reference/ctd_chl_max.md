# Depth of the chlorophyll-a maximum (DCM) of one CTD cast

The provider's definition (Rasmus Swalethorp, 2026-09-23, questions.csv
Q02, adopted 2026-10-01): "To avoid a potential narrow spike being
adopted as the max I suggest we do a 3 m running mean (I was considering
5m but sometimes the layers can be pretty narrow), and call the depth of
the highest value within that running mean the DCM"
(CalCOFI/workflows#102). Use the bottle-fitted sensor estimate
`est_chlorophyll_a_sta_corr` (Rasmus Swalethorp, 2026-09-09). This
replaces the 5-sample running median of calcofi4db 4.16.0-4.17.x.

## Usage

``` r
ctd_chl_max(depth, chl, qual = NA, window_m = 3)
```

## Arguments

- depth:

  depth (m), one cast.

- chl:

  chlorophyll-a (mg m^-3).

- qual:

  quality codes of `chl` (`NA` = good).

- window_m:

  running-mean window (m), default 3; `0` takes the raw maximum.

## Value

one-row tibble: `chl_max_depth_m`, `chl_max_value` (the running mean
there), `n`.

## Details

The window is in metres of depth, not in samples: each good sample's
smoothed value is the mean of the good samples within `window_m / 2` of
it (on the 1 m bins, the bin and its two neighbours), so an uneven grid
or a gap is averaged over what the water column actually has. The answer
is silent on the ends of the profile and on ties, so: the window is
truncated at the top and bottom (the shallowest bin averages itself and
the bin below), and among depths tied at the smoothed maximum the one
whose raw value is highest wins, then the shallowest (the 4.16.0 tie
rule; a flat profile's DCM is its shallowest bin). Flagged (8/9),
missing and non-finite samples are dropped first.

## Examples

``` r
z <- 0:150
ctd_chl_max(z, 0.2 + 2 * exp(-((z - 40) / 10)^2))
#> # A tibble: 1 × 3
#>   chl_max_depth_m chl_max_value     n
#>             <dbl>         <dbl> <int>
#> 1              40          2.19   151
```
