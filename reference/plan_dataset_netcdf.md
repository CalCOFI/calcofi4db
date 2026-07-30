# Plan the netCDF shape for a dataset

Decides whether a dataset publishes as a **flat CF Discrete Sampling
Geometry profile** or as a **nested netCDF-4 group hierarchy**, and
enumerates the variable groups either way. Replaces the per-dataset
judgement previously baked into each `publish_*_to-netcdf.qmd`.

## Usage

``` r
plan_dataset_netcdf(con, dataset_key, obs_tbl = "obs")
```

## Arguments

- con:

  DuckDB connection carrying the core tables

- dataset_key:

  Dataset provenance stamp

- obs_tbl:

  Observation table to plan from (default `"obs"`); pass
  `"obs_ctd_full"` to plan the supplemental full-resolution CTD scans.

## Value

A list with:

- dataset_key, obs_tbl:

  echoed inputs

- levels:

  the
  [`discover_sample_levels()`](https://calcofi.io/calcofi4db/reference/discover_sample_levels.md)
  tibble

- shape:

  `"profile"` or `"groups"`

- feature_type:

  `"profile"` for CF DSG, else `NA`

- has_depth_axis:

  whether `obs` carries a usable depth axis

- measurement_types:

  every `measurement_type` in `obs_tbl`, the union across ALL partitions

- attribute_types:

  `obs_attribute` measurement types, each of which becomes its own group
  (they carry different units)

- effort_types:

  `sample_measurement` types, widened onto their level

## Details

**Shape rule.** One sampling level plus a depth axis is exactly a CF
profile, so it is emitted as one (`featureType=profile`, contiguous
ragged array) and needs no extension to the standard. More than one
level has no CF feature type, so it becomes netCDF-4 groups with
explicit `parent_index` links. Being explicit about which half of that
split a file falls in is what lets the file claim CF compliance honestly
rather than approximately.

**`measurement_types` is a union, deliberately.** Sampling one partition
is how `ctd-cast_full.nc` came to declare 32 of 54 variables: bottle
nutrients were not folded into the CTD files until 2008, so the
alphabetically-first cruise (1998) simply had no column for them, and
every later-introduced type was silently dropped from a file advertised
as full resolution.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- cc_get_db()
plan_dataset_netcdf(con, "calcofi_ctd-cast")$shape   # "profile"
plan_dataset_netcdf(con, "swfsc_ichthyo")$shape      # "groups"
} # }
```
