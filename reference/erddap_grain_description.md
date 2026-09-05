# What one ERDDAP grain means, in a sentence

What one ERDDAP grain means, in a sentence

## Usage

``` r
erddap_grain_description(grain)
```

## Arguments

- grain:

  a grain from `.erddap_grain()` (`"observations"`, `"sampling events"`,
  `"length/stage frequency"`, `"full resolution (pre-thinning)"`, or a
  suffix the generic publisher coined)

## Value

character; `NA_character_` for a grain with no registered sentence,
which
[`check_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/check_dataset_catalog.md)
reports as `grain_without_description`.
