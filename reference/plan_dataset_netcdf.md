# Plan the netCDF shape for a dataset

Decides whether a dataset publishes as a **flat CF Discrete Sampling
Geometry profile** or as a **nested netCDF-4 group hierarchy**, and
enumerates the variable groups either way. Replaces the per-dataset
judgement previously baked into each `publish_*_to-netcdf.qmd`.

## Usage

``` r
plan_dataset_netcdf(
  con,
  dataset_key,
  obs_tbl = "obs",
  moving_sample_types = "underway"
)
```

## Arguments

- con:

  DuckDB connection carrying the core tables

- dataset_key:

  Dataset provenance stamp

- obs_tbl:

  Observation table to plan from (default `"obs"`); pass
  `"obs_ctd_full"` to plan the supplemental full-resolution CTD scans.

- moving_sample_types:

  `sample_type` values that denote a moving platform, and so a CF
  trajectory rather than a collection of points.

## Value

A list with:

- dataset_key, obs_tbl:

  echoed inputs

- levels:

  the
  [`discover_sample_levels()`](https://calcofi.io/calcofi4db/reference/discover_sample_levels.md)
  tibble

- shape:

  `"profile"`, `"trajectory"`, `"point"` or `"groups"`

- feature_type:

  the CF `featureType`, or `NA` for `"groups"`

- has_depth_axis:

  whether `obs` carries a usable depth axis

- depths_per_instance:

  median distinct depths per sampling instance — the discriminator
  between a vertical profile and a single-depth event

- measurement_types:

  every `measurement_type` in `obs_tbl`, the union across ALL partitions

- attribute_types:

  `obs_attribute` measurement types, each of which becomes its own group
  (they carry different units)

- effort_types:

  `sample_measurement` types, widened onto their level

## Details

**Shape rule.** Four outcomes, decided from the data:

|  |  |  |  |  |
|----|----|----|----|----|
| levels | depths per instance | sample_type | shape | CF |
| 1 | \> 1 | any | `profile` | `featureType=profile`, ragged array |
| 1 | \<= 1 | `underway` | `trajectory` | `featureType=trajectory`, ragged array per cruise |
| 1 | \<= 1 | other | `point` | `featureType=point`, one flat dimension |
| 0 or \> 1 | any | any | `groups` | none — netCDF-4 groups + `parent_index` |

A depth axis is required for `profile` but **optional** for `point`:
CF's point feature needs only time and position, so a net tow with no
recorded depth is still an honest point collection, where writing it as
a single-group netCDF-4 file would make no CF claim at all and gain
nothing.

**Why `depths_per_instance` and not merely "has a depth axis".** A CF
profile is a *vertical series at one horizontal position*. Almost every
CalCOFI dataset has a depth on its observations, but only
`calcofi_ctd-cast` has many depths per event (median 74); a tow, a
transect, an underway record and a region pool each carry a single
depth. Deciding on "one level + a depth axis" alone therefore stamped
`featureType=profile` on 10 of 15 datasets that are nothing of the kind
— a file that claims a feature type it does not have is worse than one
that claims none, because CF-aware tools act on the claim.

**Why `underway` is named explicitly.** `sample_type` is a controlled
vocabulary in the core model
(site/tow/net/cast/bottle/underway/transect/ region_pool), and a moving
platform is not inferable from row counts: an underway series looks
exactly like a collection of points until you know the platform was
under way between them. Naming the one vocabulary term that means
"moving" is configuration, not a special case.

More than one level has no CF feature type at all, so it becomes
netCDF-4 groups with explicit `parent_index` links. Being explicit about
which of the four a file falls in is what lets it claim CF compliance
honestly rather than approximately.

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
plan_dataset_netcdf(con, "calcofi_mets")$shape       # "trajectory"
plan_dataset_netcdf(con, "cce-lter_zooscan")$shape   # "point"
plan_dataset_netcdf(con, "swfsc_ichthyo")$shape      # "groups"
} # }
```
