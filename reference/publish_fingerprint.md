# Fingerprint what one dataset's published output is a function of

Fingerprint what one dataset's published output is a function of

## Usage

``` r
publish_fingerprint(data = character(), metadata = list(), code = character())
```

## Arguments

- data:

  named character from
  [`publish_data_parts()`](https://calcofi.io/calcofi4db/reference/publish_data_parts.md)

- metadata:

  named list of anything else the output reads — the record digest from
  [`publish_record_digest()`](https://calcofi.io/calcofi4db/reference/publish_record_digest.md),
  a registry table, a sidecar list; each element is digested and named
  `meta:{name}`

- code:

  named character from
  [`publish_code_parts()`](https://calcofi.io/calcofi4db/reference/publish_code_parts.md)

## Value

A list shaped like
[`input_fingerprint()`](https://calcofi.io/calcofi4db/reference/input_fingerprint.md)'s
— `hash` and named `parts` — so
[`changed_inputs()`](https://calcofi.io/calcofi4db/reference/changed_inputs.md)
reports what moved.
