# Build the `versions.json` register of every release under a prefix

Discovers each `{prefix}/{version}/catalog.json` in the bucket and
returns the list `release_database.qmd` writes as `versions.json` — one
record per release with `version`, `release_date`, `tables`,
`total_rows`, `size_mb`, plus the archive-policy fields: `consolidated`
(parquet kept indefinitely, from `metadata/release_policy.yml`) and,
when the version's parquet has been removed by
`scripts/thin_releases.R`, `retired` (`{retired_utc, to, reason}` read
from its `retired.json`). Both the release notebook and the thinning
script call this, so a re-run of the release cannot drop the policy
fields.

## Usage

``` r
build_versions_json(
  bucket,
  prefix = CC_RELEASE_PREFIX,
  consolidated = character(),
  current = NULL
)
```

## Arguments

- bucket:

  GCS bucket

- prefix:

  release prefix (`ducklake/releases`, or a staging prefix)

- consolidated:

  character vector of consolidated versions

- current:

  optional record for the release being written right now (takes
  precedence over what the bucket holds for that version)

## Value

list of release records, newest first
