# Define a CF Discrete-Sampling-Geometry profile file

A single sampling level with a depth axis *is* a CF profile, so it is
written as one — `featureType=profile` with a contiguous ragged array —
and needs no extension to the standard. This defines the dimensions and
variables; feed the `vars` to
[`ncdf4::nc_create()`](https://rdrr.io/pkg/ncdf4/man/nc_create.html) and
then call
[`nc_profile_write()`](https://calcofi.io/calcofi4db/reference/nc_profile_write.md)
one or more times.

## Usage

``` r
nc_profile_def(
  n_profile,
  n_obs,
  profile_proto,
  obs_types,
  var_meta = list(),
  strlen = 64L
)
```

## Arguments

- n_profile:

  Number of profiles (the instance dimension).

- n_obs:

  Total number of depth levels across all profiles.

- profile_proto:

  data.frame whose **columns and types** define the profile-level
  variables (the rows are ignored, so a zero-row frame or the full frame
  both work). Typically `profile_id`, `time`, `latitude`, `longitude`
  plus keys such as `cruise_key`.

- obs_types:

  Character vector of obs-level measurement variable names — each
  becomes its own double variable, which is the point of the widening.

- var_meta:

  Named list from
  [`measurement_var_meta()`](https://calcofi.io/calcofi4db/reference/measurement_var_meta.md).

- strlen:

  Fixed character length for string variables.

## Value

`list(dims = list(profile, obs, strlen), vars = <named list>)`. `vars`
always includes `rowSize` (the ragged-array index) and `depth`.

## Details

Dimensions must be sized at creation time, which is why
`n_profile`/`n_obs` are arguments rather than being inferred from the
data: a multi-hundred-million row table is written in chunks, and a
wrong guess means rewriting a multi-GB file. Size them with a cheap
counting pass first.
