# Write the CF Discrete-Sampling-Geometry attributes of a profile file

These attributes are what make the file a CF *profile dataset* rather
than a table that happens to be stored in netCDF: `cf_role` marks the
instance identifier and `sample_dimension` on `rowSize` declares the
contiguous ragged array. Without them a CF-aware reader sees two
unrelated dimensions.

## Usage

``` r
nc_profile_atts(
  nc,
  obs_types,
  var_meta = list(),
  profile_vars = character(),
  profile_id_var = "profile_id"
)
```

## Arguments

- nc:

  Open `ncdf4` handle.

- obs_types:

  Obs-level measurement variable names.

- var_meta:

  Named list from
  [`measurement_var_meta()`](https://calcofi.io/calcofi4db/reference/measurement_var_meta.md).

- profile_vars:

  Profile-level variable names present in the file; the coordinate ones
  among them get their `standard_name`/`axis`.

- profile_id_var:

  The instance-identifier variable.

## Value

`TRUE`, invisibly.
