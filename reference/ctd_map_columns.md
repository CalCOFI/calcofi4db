# Map an uploaded file's columns onto measurement types

Two vocabularies, one answer. A CalCOFI `.csv` maps through
`measurement_type.csv`'s `_source_column`, which already holds exactly
those names; a Sea-Bird file maps through `metadata/sbe_name_map.csv`.

## Usage

``` r
ctd_map_columns(
  cols,
  d_meas_type,
  d_sbe_map = NULL,
  format = c("csv", "cnv", "asc", "btl")
)
```

## Arguments

- cols:

  column names of the uploaded data

- d_meas_type:

  the measurement registry
  ([`read_measurement_type()`](https://calcofi.io/calcofi4db/reference/read_measurement_type.md))

- d_sbe_map:

  the Sea-Bird crosswalk (`metadata/sbe_name_map.csv`)

- format:

  one of `"csv"`, `"cnv"`, `"asc"`, `"btl"`

## Value

a tibble of `column`, `measurement_type`, `role`, `qual_column`,
`units`, `note` — one row per uploaded column, `role = "unmapped"` where
nothing matched

## Details

UNMAPPED COLUMNS ARE A RESULT, NOT AN ERROR. They are where a format
change announces itself — a renamed sensor, a new instrument, a column
nobody has seen before — so they are returned and shown rather than
dropped quietly.
