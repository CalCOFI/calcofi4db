# Read `metadata/measurement_face.csv`

A registry authored before `short_label` existed (4.26.0) still reads:
the column is then `NA` for every key.

## Usage

``` r
read_measurement_face(path, validate = TRUE)
```

## Arguments

- path:

  path to `metadata/measurement_face.csv`

- validate:

  error on sentinel strings (default TRUE)

## Value

tibble with columns `key`, `short_label`, `face_kind`, `face_of`,
`stands_in_note`, `source`
