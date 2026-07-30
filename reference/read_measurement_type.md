# Read `metadata/measurement_type.csv`, refusing a corrupted registry

The canonical measurement vocabulary, validated on the way in via
[`check_registry_na_strings()`](https://calcofi.io/calcofi4db/reference/check_registry_na_strings.md)
so a `write_csv(na = "NA")` round trip fails here instead of silently
reaching the release.

## Usage

``` r
read_measurement_type(path, validate = TRUE)
```

## Arguments

- path:

  path to `metadata/measurement_type.csv`

- validate:

  error on sentinel strings (default TRUE). Only set FALSE to inspect a
  file you already know is broken.

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html) of the
registry, with empty cells as `NA`.

## Details

The read is deliberately **strict**: `na = ""`, so only genuinely empty
cells become `NA`. `read_csv()`'s default is `na = c("", "NA")`, which
converts the literal string `"NA"` back to `NA` — meaning a default read
*cannot see* this corruption, and no validator downstream of one ever
could. DuckDB, which is not so forgiving, is where the damage surfaces.
