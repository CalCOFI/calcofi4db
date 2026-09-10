# Why two measurement keys sharing a NERC P01 concept are kept apart

P01 identity says two series name the same quantity. It does not say
they may be pooled, and `related[]` is where the record states the
difference the crosswalk rule (plan 2026-09-10 § D3) refused to merge
across:

## Usage

``` r
measurement_related_reasons()
```

## Value

A character vector, the `related[].why` enum.

## Details

- `underway_vs_cast` — one side is an underway intake, the other a cast
  (`sst_c` beside `temperature`). Different sampling entirely.

- `same_bottles` — one side is a `btl_*` / `*_btl` series: another
  dataset's files carrying their own copy of the bottle table. Plausibly
  the same physical bottles, so merging them would double count.

- `replicate_vs_mean` — one side is a `…_rep` replicate, the other a
  mean (`alkalinity_rep1` beside `alkalinity`).

- `pre_qc_twin` — one side is an `r_*` series reported before quality
  control.

- `sensor_vs_mean` — the two share a dataset and exactly one side's
  registry `derivation` starts with "Mean of": a raw sensor beside the
  mean of the sensor pair (`oxygen_ml_l_1` beside
  `oxygen_ml_l_ave_sta_corr`, which is the `oxygen_ml_l` key). One is an
  input to the other.

- `paired_sensors` — the two sensors of one instrument: the same dataset
  and the same P01, and neither side is the mean (`oxygen_ml_l_1` beside
  `oxygen_ml_l_2`). Two readings of one water sample, not two
  measurements.

- `same_casts` — the residual same-quantity case: different datasets,
  neither underway, and no bottle-table, replicate, pre-QC or sensor
  marker to name a sharper difference. Plausibly the same water sampled
  on the same casts, and kept apart for that reason — the DIC package's
  `salinity_pss78`, which is the CTD salinity of the DIC casts, beside
  the unified `salinity`.

They are tested in that order, because the first that applies is the
coarsest difference: an underway intake beside a cast's bottle table is
`underway_vs_cast`, not `same_bottles`.

A pair sharing a P01 that matches none of the seven gets **no**
`related[]` entry: the vocabulary states no reason, and the record never
invents one.

## Examples

``` r
measurement_related_reasons()
#> [1] "underway_vs_cast"  "same_bottles"      "replicate_vs_mean"
#> [4] "pre_qc_twin"       "sensor_vs_mean"    "paired_sensors"   
#> [7] "same_casts"       
```
