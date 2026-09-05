# The ERDDAP global attributes of one dataset, from the same record

Plan § D-8: ERDDAP's globals, the DwC-A's EML, the EDI package's EML and
the page's JSON-LD are rendered from one record, so none of them is
typed twice. `infoUrl` is the dataset page. `creator_*` follow the same
order
[`build_eml()`](https://calcofi.io/calcofi4db/reference/build_eml.md)
does (creators → pi_names → the provider organization); `creator_email`
falls back to
[`eml_contact_address()`](https://calcofi.io/calcofi4db/reference/eml_contact_address.md).

## Usage

``` r
erddap_globals(record)
```

## Arguments

- record:

  one dataset record

## Value

A named character vector of ERDDAP global attributes (absent values are
omitted, never blank).
