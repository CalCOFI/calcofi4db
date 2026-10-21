# One release's record in `versions.json`, from its `catalog.json`

One release's record in `versions.json`, from its `catalog.json`

## Usage

``` r
versions_record(catalog)
```

## Arguments

- catalog:

  a parsed `catalog.json` (list)

## Value

list: `version`, `release_date`, `tables`, `total_rows`, `size_mb`, and
`doi` once
[`publish_release_notes()`](https://calcofi.io/calcofi4db/reference/publish_release_notes.md)
has written it into the catalog
