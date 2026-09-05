# A STAC Collection for one dataset record

A STAC Collection for one dataset record

## Usage

``` r
stac_collection(rec, metadata = list(), base_url = CC_STAC_HTTPS)
```

## Arguments

- rec:

  one element of `datasets.json`'s `datasets[]`

- metadata:

  the parsed `metadata.json` (for `table:tables`)

- base_url:

  the HTTPS root the catalog is served from

## Value

A named list, the Collection document.
