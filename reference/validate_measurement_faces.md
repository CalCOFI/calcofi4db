# Validate the five measurement-face registries against Appendix A

Returns findings rather than stopping (mirrors
[`check_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/check_measurement_bounds.md)):
a workstream fills the registries, runs this, and resolves every row it
reports before calling itself done. Checks, one row of output each:

## Usage

``` r
validate_measurement_faces(
  chem_path,
  method_path,
  scale_path,
  why_path,
  face_path,
  measurement_type_path = NULL
)
```

## Arguments

- chem_path, method_path, scale_path, why_path, face_path:

  paths to the five registries

- measurement_type_path:

  path to `metadata/measurement_type.csv`, used to check
  `measurement_face` keys exist. `NULL` skips that check.

## Value

a tibble of findings (`registry`, `key`, `rule`, `detail`); zero rows
means clean.

## Details

- a `measurement_chem` / `measurement_method` / `measurement_scale` /
  `measurement_face` row with an empty `source`

- a value outside its column's Appendix A vocabulary (chem `role`/`via`,
  method `platform`, scale `kind`, why `kind`, face `face_kind`)

- a `measurement_why` key with zero, or more than one, `rank == 1` row

- a `measurement_face` key absent from `measurement_type.csv`

- a `nerc_l22` (method) that is not an exact-form L22 concept URI
