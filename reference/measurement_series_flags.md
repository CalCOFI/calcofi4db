# The `flags[]` a `measurements.json` series can carry

A `series[]` entry is one `measurement_type × dataset` of `obs_env` —
what a dataset calls a measurement. A flag says something about that
series a reader of the page needs in order to read its numbers
correctly, so a measurement page can show a quiet pill rather than a
footnote nobody writes:

## Usage

``` r
measurement_series_flags()
```

## Value

A character vector, the `series[].flags` enum of
`measurements.schema.json`.

## Details

- `sensor_mean` — the registry's `derivation` says the series is a mean
  of a sensor pair (`temperature_ave`, "Mean of the two temperature
  sensors"). The per-sensor series ride the full-resolution product.

- `replicate` — a `…_rep1` / `…_rep2` replicate beside a mean of the
  same quantity (`alkalinity_rep1`), never itself the mean.

- `reported_pre_qc` — an `r_*` series: the value the source reported
  before quality control, kept beside the QC'd one.

- `no_bound` — the registry declares neither `valid_min` nor
  `valid_max`, so nothing in the pipeline can call one of its values
  impossible.

- `sentinel_suspected` — a value the series' own registry row says is
  impossible, or one that looks like a fill where nothing is declared.
  Precisely: `out_of_bounds$n > 0` (at least one value outside the
  declared `valid_min` / `valid_max`), or — with no bound declared at
  all — an observed maximum both at or above `CC_MEASUREMENT_SENTINEL`
  (99, the shape a 9-fill takes) **and** more than
  `CC_MEASUREMENT_SENTINEL_RATIO` (100) times the series' own 95th
  percentile. Both halves of the second test are needed: PAR reads
  14,187 uE/m2/s and short-wave radiation 1,456 W/m2 legitimately, while
  the METS `sst_c` of v2026.09.06 reads 9,895 degC against a 95th
  percentile of 20.4. The minimum is tested the same way — at or below
  -99 and more than 100x the 5th percentile in magnitude — which is what
  catches the CTD `spar` of -3.07e17.

- `no_flag_at_grain` — the registry names no `_qual_column`, so the
  series reaches the release carrying no quality code. The CTD's
  `temperature_ave` is the headline case: the sensor flags ride
  `temperature_1` / `temperature_2` in `obs_ctd_full`.

- `no_p01` — no `nerc_p01` concept. Under the registry's exact-match
  rule that means "no concept says exactly this", never "not looked at".

## Examples

``` r
measurement_series_flags()
#> [1] "sensor_mean"        "replicate"          "reported_pre_qc"   
#> [4] "no_bound"           "sentinel_suspected" "no_flag_at_grain"  
#> [7] "no_p01"            
```
