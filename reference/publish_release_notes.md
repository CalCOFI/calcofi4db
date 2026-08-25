# Render and (re)publish RELEASE_NOTES.md for a version

Notes-only: renders from `RELEASES.md` + the version's local sidecars,
writes `dir_releases/{version}/RELEASE_NOTES.md`, and uploads it and
`RELEASES.md` to the bucket with `cache-control: no-cache`. Safe to run
for any version at any time — it never touches data, `catalog.json` or
`latest.txt`.

## Usage

``` r
publish_release_notes(
  version,
  releases_md,
  dir_releases,
  bucket = "calcofi-db",
  pkg_versions = NULL
)
```

## Arguments

- version:

  the release.

- releases_md:

  path to RELEASES.md.

- dir_releases:

  the local `data/releases` directory.

- bucket:

  GCS bucket (default `"calcofi-db"`); `NULL` to skip upload.

- pkg_versions:

  see
  [`render_release_notes()`](https://calcofi.io/calcofi4db/reference/render_release_notes.md).

## Value

Invisibly, the local RELEASE_NOTES.md path.
