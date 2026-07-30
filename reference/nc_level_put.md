# Write the data and attributes for one level defined by [`nc_level_vars()`](https://calcofi.io/calcofi4db/reference/nc_level_vars.md)

Write the data and attributes for one level defined by
[`nc_level_vars()`](https://calcofi.io/calcofi4db/reference/nc_level_vars.md)

## Usage

``` r
nc_level_put(
  nc,
  group,
  df,
  vars,
  parent_index = NULL,
  var_meta = list(),
  parent_group = NA_character_
)
```

## Arguments

- nc:

  Open `ncdf4` handle.

- group:

  Group name, matching the
  [`nc_level_vars()`](https://calcofi.io/calcofi4db/reference/nc_level_vars.md)
  call.

- df:

  The same data.frame passed to
  [`nc_level_vars()`](https://calcofi.io/calcofi4db/reference/nc_level_vars.md).

- vars:

  The
  [`nc_level_vars()`](https://calcofi.io/calcofi4db/reference/nc_level_vars.md)
  result.

- parent_index:

  1-based parent index, or `NULL` at the root.

- var_meta:

  Named list from
  [`measurement_var_meta()`](https://calcofi.io/calcofi4db/reference/measurement_var_meta.md).

- parent_group:

  Name of the parent group, used in the `parent_index` documentation
  attributes.

## Value

`TRUE`, invisibly.
