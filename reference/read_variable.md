# Read `metadata/variable.csv`, refusing a corrupted registry

Read `metadata/variable.csv`, refusing a corrupted registry

## Usage

``` r
read_variable(path, validate = TRUE)
```

## Arguments

- path:

  path to `metadata/variable.csv`

- validate:

  error on sentinel strings (default TRUE). Only set FALSE to inspect a
  file you already know is broken.

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html) of the
registry, with empty cells as `NA`.
