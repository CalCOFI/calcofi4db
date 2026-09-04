# Replace a measurement type's definition while keeping its curated columns

Ten ingests "register" their types by deleting the existing row and
binding a freshly-built literal in its place:

## Usage

``` r
upsert_measurement_types(
  d,
  new_types,
  preserve = c("valid_min", "valid_max", "valid_depth_min_m", "valid_depth_max_m"),
  authoritative = c(declarable_measurement_fields(), "denominator")
)
```

## Arguments

- d:

  the current registry (a data.frame, e.g. from
  [`read_measurement_type()`](https://calcofi.io/calcofi4db/reference/read_measurement_type.md))

- new_types:

  data.frame of definitions to upsert; needs `measurement_type`

- preserve:

  columns to carry forward from the existing row when `new_types` does
  not supply a non-`NA` value. Defaults to the bound columns — the ones
  an ingest never authors and a provider has agreed.

- authoritative:

  registry-owned columns (default
  [`declarable_measurement_fields()`](https://calcofi.io/calcofi4db/reference/declarable_measurement_fields.md)
  plus `denominator`, the D8 effort vocabulary): the existing registry
  value wins whenever it is non-NA, even over an explicit value in
  `new_types`, because only
  [`declare_measurement_fields()`](https://calcofi.io/calcofi4db/reference/declare_measurement_fields.md)
  may set them. A type new to the registry takes the literal's value.

## Value

The updated registry, sorted by `measurement_type`.

## Details

    d_meas_type |> filter(measurement_type != "euphausiid_abundance") |>
      bind_rows(euph_types)          # <- literal, no valid_min/valid_max

Every column the literal omits is destroyed on each re-run. That is how
a provider-agreed `valid_min` silently disappeared from
`euphausiid_abundance` and the four picoplankton types during the
v2026.08.08 re-render: the ingests had not changed, but a curated column
had been added underneath them. Only `ingest_calcofi_mets.qmd` did the
preserve-and-merge dance by hand.

Use this instead of `filter(... != x) |> bind_rows(new)`. It replaces
the definition columns the ingest owns and carries the curated ones
forward from the row being replaced, so a re-run cannot quietly narrow
the registry.

## See also

[`declare_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/declare_measurement_bounds.md)
to set a bound,
[`register_measurement_types()`](https://calcofi.io/calcofi4db/reference/register_measurement_types.md)
to append a genuinely new type.

## Examples

``` r
if (FALSE) { # \dontrun{
d_meas_type <- upsert_measurement_types(d_meas_type, euph_types)
readr::write_csv(d_meas_type, meas_type_csv, na = "")
} # }
```
