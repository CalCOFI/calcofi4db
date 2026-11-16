# The file name of a dataset's bundle

`calcofi_{dataset_key}_{version}_{format}.zip`, the `calcofi_` prefix
omitted when the key already starts with it
(`calcofi_bottle_v2026.11.14_csv.zip`, not `calcofi_calcofi_bottle_…`),
so the name says what it is when it lands in Downloads.

## Usage

``` r
bundle_file_name(dataset_key, version, format, ext = ".zip")
```

## Arguments

- dataset_key:

  the dataset

- version:

  the release the bundle was built from

- format:

  one of
  [`bundle_formats()`](https://calcofi.io/calcofi4db/reference/bundle_formats.md)

- ext:

  the extension; `""` gives the stem (the folder inside the zip)

## Value

Character.
