# Read a Sea-Bird `.cnv` converted data file

The `# name N = short: long [units]` header names every column
explicitly, so unlike `.asc` there is nothing to infer. `# bad_flag`
(conventionally `-9.990e-29`) becomes `NA` — the same pseudo-NA the
CalCOFI ingest already strips from coordinates.

## Usage

``` r
read_sbe_cnv(path)
```

## Arguments

- path:

  file path

## Value

a tibble with `sbe_header` and `sbe_units` attributes
