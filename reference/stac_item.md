# A STAC Item for one dataset at one release

A STAC Item for one dataset at one release

## Usage

``` r
stac_item(
  rec,
  metadata = list(),
  version = NULL,
  release_date = NULL,
  base_url = CC_STAC_HTTPS
)
```

## Arguments

- rec:

  one element of `datasets.json`'s `datasets[]`

- metadata:

  the parsed `metadata.json` (for `table:tables`)

- version:

  the release version (the Item id)

- release_date:

  the release date (`datetime`/`created`)

- base_url:

  the HTTPS root the catalog is served from

## Value

A named list, the Item document.
