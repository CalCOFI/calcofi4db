# Generate `meta.xml` for an Event-core archive

Maps each written CSV's columns to their Darwin Core term URIs. **A
column with no term is an error**: the ichthyo notebook only
[`message()`](https://rdrr.io/r/base/message.html)d about one, so a
renamed column would have shipped as a field the IPT silently ignores.

## Usage

``` r
dwc_meta_xml(tables, terms = dwc_term_map())
```

## Arguments

- tables:

  a named list of the archive's data frames: `event` (the core) and any
  of `occurrence`, `emof`

- terms:

  the map from
  [`dwc_term_map()`](https://calcofi.io/calcofi4db/reference/dwc_term_map.md)

## Value

The `meta.xml` document as a length-1 character string.

## See also

[`dwc_archive()`](https://calcofi.io/calcofi4db/reference/dwc_archive.md)
