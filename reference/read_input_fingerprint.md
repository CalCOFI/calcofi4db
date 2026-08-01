# Read a previously recorded input fingerprint

Read a previously recorded input fingerprint

## Usage

``` r
read_input_fingerprint(path)
```

## Arguments

- path:

  JSON file written by
  [`write_input_fingerprint()`](https://calcofi.io/calcofi4db/reference/write_input_fingerprint.md)

## Value

the recorded list (`hash`, `parts`, `recorded_at`), or `NULL` when the
file is absent or unreadable — both mean "no usable prior state", which
must fall through to a full run rather than error
