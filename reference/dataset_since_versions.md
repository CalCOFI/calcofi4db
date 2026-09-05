# The first release each dataset appeared in

Walks `versions.json` oldest → newest and reads each version's
`metadata.json` (retired versions keep their sidecars), recording the
first version whose `datasets` block names the key. ~30 small fetches at
release time; the tests inject a `fetch` over fixtures.

## Usage

``` r
dataset_since_versions(
  versions,
  base = CC_RELEASES_HTTPS,
  fetch = NULL,
  known = NULL
)
```

## Arguments

- versions:

  the parsed `versions.json` (the list under `versions`, or the whole
  object)

- base:

  the HTTPS releases prefix

- fetch:

  the HTTP function

- known:

  a named character vector from a previous `datasets.json`
  (`dataset_key -> since_version`); those keys are not re-derived

## Value

A named character vector `dataset_key -> version`.
