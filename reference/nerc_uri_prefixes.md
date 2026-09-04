# The NERC NVS collections [`declare_measurement_fields()`](https://calcofi.io/calcofi4db/reference/declare_measurement_fields.md) validates against

Maps each vocabulary column to the collection its URIs must come from. A
typo in a concept id is invisible (it is just a string), but a URI in
the wrong collection — a P06 unit where a P01 parameter belongs — is
exactly the kind of mistake that reaches a portal export intact, so the
prefix is checked.

## Usage

``` r
nerc_uri_prefixes()
```

## Value

Named character vector: column name -\> required URI prefix.
