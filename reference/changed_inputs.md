# Which inputs changed since a recorded fingerprint

Which inputs changed since a recorded fingerprint

## Usage

``` r
changed_inputs(fp, prior)
```

## Arguments

- fp:

  output of
  [`input_fingerprint()`](https://calcofi.io/calcofi4db/reference/input_fingerprint.md)

- prior:

  output of
  [`read_input_fingerprint()`](https://calcofi.io/calcofi4db/reference/read_input_fingerprint.md);
  `NULL` means everything is new

## Value

character vector of input names that were added, removed or changed
