# Depth at which a profile reaches a threshold: the nitracline and hypoxic-boundary rule

Rasmus Swalethorp's definition (email "Next Two Weeks Tasks" 2026-09-24;
the CalCOFI legacy `D_NClineDepth.m`): the nitracline is the "depth at
which nitrate is equal to or just above 1 uM. For the 1 m binned data
this is simple. For the discrete depth bottle data we will have to find
the depth between the point above and below this concentration ... by
linear interpolation between the two points". The hypoxic boundaries are
"the same as nitracline" with oxygen falling to a threshold, taking the
shallowest crossing (see
[`ctd_hypoxic_depth()`](https://calcofi.io/calcofi4db/reference/ctd_hypoxic_depth.md)).

## Usage

``` r
profile_threshold_depth(
  depth,
  value,
  qual = NA,
  threshold,
  direction = c("above", "below"),
  surface_max_m = 20,
  min_n = 3
)
```

## Arguments

- depth:

  depth (m), one cast or bottle profile.

- value:

  the quantity tested (nitrate in umol/L, oxygen in mL/L, ...).

- qual:

  quality codes of `value` (`NA` = good); 8/9 are dropped.

- threshold:

  the concentration the profile must reach.

- direction:

  `"above"`: the first depth where `value >= threshold` (a nutricline);
  `"below"`: the first depth where `value <= threshold` (a hypoxic
  boundary).

- surface_max_m:

  deepest first sample that may be reported as the crossing when it
  already reaches the threshold (m), default 20.

- min_n:

  fewest samples the profile needs for an `at_surface` answer, default
  3.

## Value

one-row tibble: `depth_m`, `value_above`, `value_below` (the bracketing
values, the same value twice for `at_surface`), `status` (`ok`,
`at_surface`, `no_bracket`, `not_reached`, `no_data`), `threshold`,
`direction`, `depth_max_m`, `n`.

## Details

The rule, as the legacy script applies it: the profile is cleaned
([`ctd_mld()`](https://calcofi.io/calcofi4db/reference/ctd_mld.md)'s
rule: flagged 8/9, missing and non-finite samples dropped, duplicate
depths averaged, sorted), then the shallowest sample `i` that reaches
the threshold is found — `value >= threshold` for `direction = "above"`
(nitrate; the script tests `NO3 > 0.9999`, so a tolerance of 1e-4 is
allowed), `value <= threshold` for `"below"` (oxygen). If `i > 1` the
crossing depth is interpolated linearly between sample `i - 1` and `i`
(`status = "ok"`). If the first good sample already reaches the
threshold, its depth is the answer only when it is no deeper than
`surface_max_m` and the profile has at least `min_n` samples
(`"at_surface"`; the script: "I_NO3 == 1 && Depth \< 20 && NoDepths \>
2"); otherwise there is nothing to bracket (`"no_bracket"`). A profile
that never reaches the threshold is `"not_reached"`; fewer than two good
samples is `"no_data"`. Only the shallowest crossing is reported: an
oxygen minimum in mid-profile has a lower boundary too, which this rule
does not return (Rasmus Swalethorp, 2026-09-24: "I suggest we just focus
on the shallowest boundary depths").

## Examples

``` r
profile_threshold_depth(c(0, 10, 20, 30, 50), c(0.1, 0.2, 0.5, 1.5, 8), threshold = 1)   # 25 m
#> # A tibble: 1 × 8
#>   depth_m value_above value_below status threshold direction depth_max_m     n
#>     <dbl>       <dbl>       <dbl> <chr>      <dbl> <chr>           <dbl> <int>
#> 1      25         0.5         1.5 ok             1 above              50     5
profile_threshold_depth(c(0, 50, 100, 200), c(6, 4, 2, 1), threshold = 2.4, direction = "below")
#> # A tibble: 1 × 8
#>   depth_m value_above value_below status threshold direction depth_max_m     n
#>     <dbl>       <dbl>       <dbl> <chr>      <dbl> <chr>           <dbl> <int>
#> 1      90           4           2 ok           2.4 below             200     4
```
