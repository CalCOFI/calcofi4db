# Record an input fingerprint next to an ingest's outputs

Written only after the outputs it describes are complete: a fingerprint
saved before the run finishes would let the next render skip a heavy
path that never actually produced anything.

## Usage

``` r
write_input_fingerprint(path, fp)
```

## Arguments

- path:

  JSON file to write

- fp:

  output of
  [`input_fingerprint()`](https://calcofi.io/calcofi4db/reference/input_fingerprint.md)

## Value

`path`, invisibly
