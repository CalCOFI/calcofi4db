# Stop on any non-exempt error finding from [`check_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/check_dataset_catalog.md)

Stop on any non-exempt error finding from
[`check_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/check_dataset_catalog.md)

## Usage

``` r
assert_dataset_catalog(d, quiet = FALSE)
```

## Arguments

- d:

  the table from
  [`check_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/check_dataset_catalog.md)

- quiet:

  suppress the messages for warn-level and exempt rows

## Value

`d`, invisibly, when nothing blocks.
