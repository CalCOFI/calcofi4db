# Read `metadata/gear.csv`, the net-gear registry

One row per `sample.tow_type` code with `gear_name`, the
`dwc_samplingProtocol` sentence a Darwin Core / EML sampling description
needs, the NERC L22 device URI where one is exact, the `datasets` that
use the code (`;`-separated) and a `note`.
[`build_eml()`](https://calcofi.io/calcofi4db/reference/build_eml.md)
appends each of a dataset's protocol sentences to its
`samplingDescription`.

## Usage

``` r
read_gear_registry(path)

dataset_gear(gear, dataset_key)
```

## Arguments

- path:

  path to `metadata/gear.csv`

- gear:

  the tibble from `read_gear_registry()` (or NULL)

- dataset_key:

  the dataset to filter to

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html), all
columns character.

`dataset_gear()`: the rows whose `datasets` names `dataset_key`.
