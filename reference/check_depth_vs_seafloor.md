# Find samples deeper than the seafloor at their position

For every root sample (no parent) takes the deepest depth attributed to
it — its own `depth_max_m`/`depth_min_m`, its descendants' and its
observations' — and compares it with the deepest GEBCO cell in the 3x3
neighbourhood of its position plus `tolerance_m`. Positions outside the
raster are `unknown`, not violations.

## Usage

``` r
check_depth_vs_seafloor(
  con,
  seafloor,
  sample_tbl = "sample",
  obs_tbl = "obs",
  tolerance_m = 10,
  nominal_datasets = character()
)
```

## Arguments

- con:

  DBI connection holding `sample_tbl` (with `parent_sample_key`,
  `root_sample_key`) and optionally `obs_tbl`.

- seafloor:

  Result of
  [`sample_seafloor()`](https://calcofi.io/calcofi4db/reference/sample_seafloor.md)
  (or a GEBCO tif path, in which case it is computed).

- sample_tbl, obs_tbl:

  Table names; `obs_tbl` may be absent.

- tolerance_m:

  Metres a sample may exceed the neighbourhood-deepest cell before it is
  a finding (default 10).

- nominal_datasets:

  `dataset_key`s whose depths are nominal maxima, measured apart from
  the rest (default none: every depth is in the main measure).

## Value

A tibble of violators — `sample_key`, `dataset_key`, `sample_type`,
`cruise_key`, `longitude`, `latitude`, `depth_m`, `seafloor_depth_m`,
`seafloor_max3x3_m`, `excess_m`, `on_land` — worst first, with
attributes `summary` (per-dataset `n_root`, `n_unknown`, `n_over`,
`max_excess_m`, `n_over_nominal`) and `nominal` (the violators by a
nominal depth alone, same columns).

## Details

A dataset named in `nominal_datasets` publishes a NOMINAL depth: the
maximum a gear could reach, not where it went (SWFSC ichthyoplankton
`Net.NetDepth`, the maximum possible depth of the net). Where the sea
floor is shallower the net simply did not get there, so that is not a
finding of the same kind. Those datasets' sample and observation depths
are left out of the main measure and measured on their own: attribute
`nominal` (same columns as the result) and `n_over_nominal` in
`summary`, so a release can ratchet each separately.
