# Promote a release to `latest.txt`

Refuses to move the pointer unless the release is structurally complete,
and writes the object with `Cache-Control: no-cache` so the change
reaches consumers immediately rather than up to an hour later. Both
behaviours exist because their absence caused an outage on 2026-08-14 —
see
[`check_release_complete()`](https://calcofi.io/calcofi4db/reference/check_release_complete.md)
and
[`read_promoted_release()`](https://calcofi.io/calcofi4db/reference/read_promoted_release.md).

## Usage

``` r
promote_release(
  version,
  bucket = "calcofi-db",
  required = RELEASE_REQUIRED_OBJECTS,
  prefix = "ducklake/releases"
)
```

## Arguments

- version:

  release version to promote

- bucket:

  GCS bucket holding `{prefix}/`

- required:

  passed to
  [`check_release_complete()`](https://calcofi.io/calcofi4db/reference/check_release_complete.md)

- prefix:

  bucket-relative releases prefix (default `ducklake/releases`)

## Value

the promoted version, invisibly
