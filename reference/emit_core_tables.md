# Project one dataset into the consolidated core tables

The per-ingest (Phase 3) entry point: after an ingest has built its
per-dataset tables, `emit_core_tables()` projects that dataset into the
shared core family — `sample` (via
[`build_sample_reference()`](https://calcofi.io/calcofi4db/reference/build_sample_reference.md),
which auto-detects the dataset's event tables present in `con`), plus
its `obs` occurrence headline, `obs_freq` (bin, count) detail, and
`sample_measurement` effort — using the same validated projection the
release assembly uses. Idempotent per connection for a single dataset's
tables. Arms for `pic_zooplankton` (no measurements) and
`calcofi_phytoplankton` (region-pooled, no grid_key) contribute `sample`
only.

## Usage

``` r
emit_core_tables(con, dataset_key, sample = TRUE)
```

## Arguments

- con:

  a DuckDB connection holding this dataset's per-dataset tables

- dataset_key:

  provider_dataset (e.g. `"swfsc_ichthyo"`, `"calcofi_bottle"`)

- sample:

  logical; also (re)build `sample` from the present event tables
  (default TRUE)

## Value

(invisibly) a named list of row counts for the core tables written
