# Define a CF Discrete-Sampling-Geometry file (profile or trajectory)

Both CF `profile` and CF `trajectory` are *contiguous ragged arrays*: an
instance dimension, an observation dimension, and a `rowSize` count
saying how many observations belong to each instance. The two differ
only in which coordinates sit on which dimension — a profile's
time/latitude/longitude are fixed per instance, while a trajectory's
vary along the track — so one writer serves both, and `obs_cols` is what
selects between them.

## Usage

``` r
nc_profile_def(
  n_profile,
  n_obs,
  profile_proto,
  obs_types,
  var_meta = list(),
  strlen = 64L,
  obs_cols = "depth"
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

- obs_cols:

  Coordinate columns that live on the **observation** dimension.
  `"depth"` (the default) gives a CF profile;
  `c("time", "latitude", "longitude", "depth")` gives a CF trajectory,
  where position varies along the track rather than being a property of
  the instance.

## Value

`list(dims = list(profile, obs, strlen), vars = <named list>)`. `vars`
always includes `rowSize` (the ragged-array index) and every `obs_cols`
entry.

## Details

This defines the dimensions and variables; feed the `vars` to
[`ncdf4::nc_create()`](https://rdrr.io/pkg/ncdf4/man/nc_create.html) and
then call
[`nc_profile_write()`](https://calcofi.io/calcofi4db/reference/nc_profile_write.md)
one or more times.

Dimensions must be sized at creation time, which is why
`n_profile`/`n_obs` are arguments rather than being inferred from the
data: a multi-hundred-million row table is written in chunks, and a
wrong guess means rewriting a multi-GB file. Size them with a cheap
counting pass first.
