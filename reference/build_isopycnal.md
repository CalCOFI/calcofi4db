# Build the release's isopycnal table: conditions on density surfaces, every CTD down cast

Calls
[`calcofi4r::cc_isopycnal()`](https://calcofi.io/calcofi4r/reference/cc_isopycnal.html)
— the one implementation of the isopycnal rule, never a copy of it —
over every `calcofi_ctd-cast` cast in the connection, `batch` cruises at
a time (the down cast of an occupation is chosen within its cruise, so
batching changes nothing), and writes the result as one table in the
release's conventions: one row per cast x surface x type, the type named
by
[`calcofi4r::cc_isopycnal_type()`](https://calcofi.io/calcofi4r/reference/cc_isopycnal_type.html)
(`iso_` +
[`cc_isopycnal()`](https://calcofi.io/calcofi4r/reference/cc_isopycnal.html)'s
variable: `iso_depth_m`, `iso_temperature_ave`, `iso_salinity_ave_corr`,
`iso_oxygen_ml_l_ave_sta_corr`, `iso_est_nitrate_sta_corr`,
`iso_spiciness0`), so its rows can never be mistaken for an `obs` series
of the same quantity at a depth.

## Usage

``` r
build_isopycnal(
  con,
  tbl = "isopycnal",
  sigma = NULL,
  variables = NULL,
  direction = "down",
  surface_max_m = NULL,
  min_n = NULL,
  batch = 20L,
  dataset_key = "calcofi_ctd-cast"
)
```

## Arguments

- con:

  DuckDB connection holding `obs_ctd_full`, `sample` and `cruise` (with
  `month_median` / `date_median`,
  [`add_cruise_median_date()`](https://calcofi.io/calcofi4db/reference/add_cruise_median_date.md)).

- tbl:

  output table (default `"isopycnal"`).

- sigma, variables, direction, surface_max_m, min_n:

  passed to
  [`calcofi4r::cc_isopycnal()`](https://calcofi.io/calcofi4r/reference/cc_isopycnal.html);
  `NULL` keeps its default (the provider's list).

- batch:

  cruises per call.

- dataset_key:

  the CTD dataset whose casts are read.

## Value

Invisibly, the row count of `tbl`. Columns: `dataset_key`, `sample_key`,
`cruise_key`, `site_key`, `grid_key`, `longitude`, `latitude`,
`datetime`, `data_stage`, `year`, `month` (the cruise's), `sigma_theta`,
`status`, `measurement_type`, `measurement_value`.

## Details

Asked for by Rasmus Swalethorp (2026-11-12/13): sigma-theta 24.0 to 27.0
by 0.2 plus 25.5 and 26.5, the shallowest crossing, depth / temperature
/ salinity / oxygen / estimated nitrate / spice on each surface, down
casts, from the 1 m bins. Those are
[`cc_isopycnal()`](https://calcofi.io/calcofi4r/reference/cc_isopycnal.html)'s
defaults, which this function passes through unchanged unless told
otherwise.

Every cast x surface keeps its `iso_depth_m` row whatever its `status`
(`"ok"`, `"at_surface"`, `"no_bracket"`, `"not_reached"`, `"no_data"`;
`measurement_value` NULL unless `"ok"`), so a surface that outcrops or
is never reached stays distinguishable from a missing cast; a property's
row is present only where the surface is `"ok"` and the property has a
value there. The table carries no `measurement_qual`: every value is
interpolated from bins the provider does not flag 8/9, after the
sensor-pair rule.

[`build_climatology()`](https://calcofi.io/calcofi4db/reference/build_climatology.md)`(isopycnal_tbl = )`
pools it into the surface baseline.
