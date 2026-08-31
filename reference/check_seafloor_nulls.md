# Classify the samples whose `seafloor_depth_m` is NULL, by cause

After
[`add_sample_seafloor()`](https://calcofi.io/calcofi4db/reference/add_sample_seafloor.md),
a `NULL` seafloor can mean four different things, and only one of them
is acceptable to ship silently. This returns one row per cause with its
count — `no_coordinates` (lon or lat NULL), `nan_coordinate`,
`outside_source_tile` (the position genuinely falls off the GEBCO tile
that was sampled) — and `inside_tile_null`, a position **inside** the
tile that still reads NULL, which can only be a regression in the
sampling itself. Gate on that one:
`stopifnot(attr(x, "n_inside_null") == 0)` (D29, 2026-08-31).

## Usage

``` r
check_seafloor_nulls(
  con,
  source_bbox = c(-180, 0, -90, 90),
  sample_tbl = "sample",
  lon_col = "longitude",
  lat_col = "latitude"
)
```

## Arguments

- con:

  DBI connection holding `sample_tbl` after
  [`add_sample_seafloor()`](https://calcofi.io/calcofi4db/reference/add_sample_seafloor.md).

- source_bbox:

  `c(w, s, e, n)` of the GEBCO source that was sampled — the full
  sub-ice tile `c(-180, 0, -90, 90)` by default.

- sample_tbl, lon_col, lat_col:

  Table and columns to read.

## Value

data.frame `cause`, `n` (plus a `datasets` summary column for the
inside-tile rows); attribute `n_inside_null` carries the gate value.
