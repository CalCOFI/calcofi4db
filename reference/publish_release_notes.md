# Render and (re)publish RELEASE_NOTES.md for a version

Notes-only: renders from `RELEASES.md` + the version's local sidecars,
writes `dir_releases/{version}/RELEASE_NOTES.md`, and uploads it and
`RELEASES.md` to the bucket with `cache-control: no-cache`. Safe to run
for any version at any time — it never touches data or `latest.txt`.

## Usage

``` r
publish_release_notes(
  version,
  releases_md,
  dir_releases,
  bucket = "calcofi-db",
  pkg_versions = NULL,
  prefix = "ducklake/releases",
  zenodo = !is.null(bucket),
  release_policy = NULL,
  fetch = NULL
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

- prefix:

  the release prefix on the bucket (a staging run passes
  `ducklake-staging/releases`).

- zenodo:

  look the version's DOI up on Zenodo (default: when uploading).

- release_policy:

  path to `metadata/release_policy.yml`, whose `consolidated` list
  [`build_versions_json()`](https://calcofi.io/calcofi4db/reference/build_versions_json.md)
  stamps when `versions.json` is rebuilt; default: beside `releases_md`.

- fetch:

  the HTTP function passed to
  [`zenodo_doi_for_tag()`](https://calcofi.io/calcofi4db/reference/zenodo_doi_for_tag.md)
  (tests).

## Value

Invisibly, the local RELEASE_NOTES.md path.

## Details

The one exception is the release's DOI. Zenodo mints it minutes after
the GitHub release is tagged, so when `zenodo = TRUE` this asks
[`zenodo_doi_for_tag()`](https://calcofi.io/calcofi4db/reference/zenodo_doi_for_tag.md)
for it and, the first time it answers, writes it into the local and
published `catalog.json` (`doi` + `citation`, via
[`add_release_citation()`](https://calcofi.io/calcofi4db/reference/add_release_citation.md);
the objects are untouched) and rebuilds `versions.json` so the version's
record carries `doi` — the notes then cite the DOI. Nothing changes when
Zenodo has no record yet or already agrees.
