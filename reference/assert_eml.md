# Stop on any non-exempt error finding from [`check_eml()`](https://calcofi.io/calcofi4db/reference/check_eml.md)

Stop on any non-exempt error finding from
[`check_eml()`](https://calcofi.io/calcofi4db/reference/check_eml.md)

## Usage

``` r
assert_eml(d, quiet = FALSE)
```

## Arguments

- d:

  the table from
  [`check_eml()`](https://calcofi.io/calcofi4db/reference/check_eml.md)
  /
  [`check_eml_catalog()`](https://calcofi.io/calcofi4db/reference/check_eml_catalog.md)

- quiet:

  suppress the messages for warn-level and exempt rows

## Value

`d`, invisibly, when nothing blocks.
