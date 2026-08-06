# Measure Observed Temporal and Spatial Coverage per Dataset

Derives each dataset's real extent from the assembled core (`sample` +
`obs`) instead of the `coverage_temporal` / `coverage_spatial` strings
an ingest asserts in its `calcofi.dataset_meta` YAML.

## Usage

``` r
observed_coverage(con, tables = c("sample", "obs"), digits = 1)
```

## Arguments

- con:

  DuckDB connection holding the assembled core.

- tables:

  Tables to measure, in order. Each contributes whichever of `datetime`
  / `latitude` / `longitude` it actually has; a table absent from the
  connection is skipped rather than erroring.

- digits:

  Decimal places for the formatted bbox label.

## Value

Tibble, one row per `dataset_key`, sorted by key: `time_min`/`time_max`
(`"YYYY-MM"`), `lat_min`/`lat_max`/`lon_min`/ `lon_max` (numeric), and
the display labels `coverage_temporal_observed` /
`coverage_spatial_observed`.

## Details

**Why measure rather than assert.** A hand-written extent cannot help
going stale — it is authored once and the data grows underneath it.
Checked against release `v2026.08.06`, the asserted temporal string was
wrong for 7 of 15 datasets: `cce-lter_zoodb` claimed coverage through
2021-05 when its data ends 2015-04, `calcofi_phyllosoma` stopped a year
short of its own rows, and three datasets said `"present"` while in fact
stalling in 2019, 2022 and 2023.

**`NaN` is not `NULL`.** A `NaN` coordinate survives `IS NOT NULL`, and
[`min()`](https://rdrr.io/r/base/Extremes.html)/[`max()`](https://rdrr.io/r/base/Extremes.html)
propagate it, so a single poisoned row would blow a dataset's whole
bounding box out to `NaN` while every nullity check passed. The
coordinate filter is `isfinite()`, which rejects `NaN` and `±Inf` alike.
See the same trap in
[`append_sample()`](https://calcofi.io/calcofi4db/reference/append_sample.md),
which normalizes these at write time.

**Absent beats invented.** A dataset with no usable datetimes gets `NA`
for the temporal half, not a guess — `calcofi_phytoplankton` is
region-pooled and carries coordinates but no `datetime`, so it
legitimately measures spatially and not temporally. Callers fall back to
a declared static value there.
