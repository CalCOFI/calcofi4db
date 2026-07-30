# Write one chunk of a CF profile file

Writes profile-level variables (one value per profile, taken from each
profile's first row), the `rowSize` ragged-array index, and the
obs-level `depth` + measurement variables. Call once for a whole
dataset, or repeatedly with advancing offsets to stream a table too
large to materialize — the 216M-row `obs_ctd_full` is written one cruise
partition at a time, holding ~15 MB rather than the whole table.

## Usage

``` r
nc_profile_write(
  nc,
  vars,
  wide,
  profile_cols,
  obs_types,
  profile_id_col = "profile_id",
  start_profile = 1L,
  start_obs = 1L,
  strlen = 64L
)
```

## Arguments

- nc:

  Open `ncdf4` handle created from
  [`nc_profile_def()`](https://calcofi.io/calcofi4db/reference/nc_profile_def.md)'s
  `vars`.

- vars:

  The `vars` element of
  [`nc_profile_def()`](https://calcofi.io/calcofi4db/reference/nc_profile_def.md).

- wide:

  Wide data.frame, one row per (profile, depth), **ordered by profile
  then depth** so each profile's rows are contiguous. Must contain
  `profile_id_col`, `depth`, and the profile-level and obs-level
  columns.

- profile_cols:

  Character vector of profile-level column names.

- obs_types:

  Character vector of obs-level measurement column names.

- profile_id_col:

  Column identifying the profile.

- start_profile, start_obs:

  1-based write offsets into the profile and obs dimensions.

- strlen:

  Fixed character length, matching
  [`nc_profile_def()`](https://calcofi.io/calcofi4db/reference/nc_profile_def.md).

## Value

`list(n_profile, n_obs)` — the counts written, for advancing the offsets
on the next chunk.

## Details

Non-contiguous profile rows are a **hard stop**, not a warning. A
contiguous ragged array encodes each profile as a run of `rowSize`
consecutive rows, so rows interleaved between profiles produce a file
that reads cleanly and assigns depths to the wrong casts. Ordering is
the caller's job (`ORDER BY profile, depth`); verifying it is this
function's.
