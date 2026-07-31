# Build the variable definitions for one level of a nested dataset

Each sampling level becomes a netCDF-4 group. The link to the parent is
an explicit index variable, **not** repetition of the parent's columns —
that is the whole reason for netCDF-4 here rather than a flat table: tow
effort is stored once, and a length-frequency bin points at the tow it
came from. Flattening instead repeats each net's `volume_sampled` onto
every one of its size bins, which turned 76,512 real ichthyo values into
369,978 repeated ones and inflated any naive `SUM()` of effort by ~5x.

## Usage

``` r
nc_level_vars(
  group,
  df,
  dim,
  parent_dim = NULL,
  parent_index = NULL,
  var_meta = list(),
  strlen = 64L
)
```

## Arguments

- group:

  Group name, e.g. `"tow"`, `"occurrence"`, `"length_bin"`. Pass `""` to
  write at the file root instead of in a group, which is what a CF
  `featureType=point` collection is — one flat dimension with no
  nesting.

- df:

  data.frame for this level, ordered so children are contiguous.

- dim:

  The `ncdim4` for this level.

- parent_dim:

  Parent level's `ncdim4`, or `NULL` at the root.

- parent_index:

  1-based index into the parent level, one per row of `df`.

- var_meta:

  Named list from
  [`measurement_var_meta()`](https://calcofi.io/calcofi4db/reference/measurement_var_meta.md).

- strlen:

  Fixed character length for string variables.

## Value

Named list of `ncvar4` objects to pass to
[`ncdf4::nc_create()`](https://rdrr.io/pkg/ncdf4/man/nc_create.html).
The parent link, when present, is the element named `__parent_index`.
