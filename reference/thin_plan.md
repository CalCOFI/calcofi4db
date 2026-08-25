# Archive-thinning plan: which versions lose their parquet

Pure policy over a `versions.json` list: keep the consolidated versions,
the promoted version and its `keep_latest - 1` predecessors; everything
else not already retired is a candidate. Each candidate is retired *to*
the nearest consolidated (or kept) version at or after it — the closest
data a reader can substitute.

## Usage

``` r
thin_plan(versions, latest, consolidated, keep_latest = 2)
```

## Arguments

- versions:

  list as returned by
  [`build_versions_json()`](https://calcofi.io/calcofi4db/reference/build_versions_json.md)
  (any order)

- latest:

  the promoted version string

- consolidated:

  character vector of consolidated versions

- keep_latest:

  how many of the newest versions to keep (default 2)

## Value

tibble `version`, `keep` (logical), `reason`, `to` (replacement for
candidates, `NA` for kept)
