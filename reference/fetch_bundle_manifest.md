# The bundle manifest of every dataset published by `publish_to-zip.qmd`

The bundle manifest of every dataset published by `publish_to-zip.qmd`

## Usage

``` r
fetch_bundle_manifest(url = CC_BUNDLE_MANIFEST_URL, fetch = NULL)
```

## Arguments

- url:

  the manifest's address

- fetch:

  the HTTP function (tests inject one)

## Value

The parsed manifest (a list with `datasets[]`), or NULL when it does not
answer.
