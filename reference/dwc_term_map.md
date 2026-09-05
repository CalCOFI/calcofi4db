# The Darwin Core term URI for every column the archive can carry

Column name -\> term URI. Most are
`http://rs.tdwg.org/dwc/terms/{column}`; the OBIS eMoF ids and the
Dublin Core terms are the exceptions (`DWC_TERM_OVERRIDE`). A column
absent from this map is an ERROR in
[`dwc_meta_xml()`](https://calcofi.io/calcofi4db/reference/dwc_meta_xml.md)
rather than a dropped field.

## Usage

``` r
dwc_term_map()
```

## Value

A named character vector, `column -> term URI`.
