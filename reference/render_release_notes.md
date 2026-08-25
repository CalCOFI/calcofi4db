# Render a version's RELEASE_NOTES.md: narrative + generated appendix

Render a version's RELEASE_NOTES.md: narrative + generated appendix

## Usage

``` r
render_release_notes(
  version,
  releases_md,
  catalog = NULL,
  metadata = NULL,
  test_results = NULL,
  pkg_versions = NULL,
  promoted = NA
)
```

## Arguments

- version:

  the release.

- releases_md:

  RELEASES.md text or lines (must contain the section).

- catalog:

  parsed `catalog.json` (list) or `NULL`.

- metadata:

  parsed `metadata.json` (list) or `NULL` — its `datasets` names are
  listed.

- test_results:

  parsed `test_results.json` (list) or `NULL`.

- pkg_versions:

  named character vector, e.g.
  `c(calcofi4db = "3.20.1", calcofi4r = "1.9.0")`, or `NULL`.

- promoted:

  whether `latest.txt` points at this version (affects one line).

## Value

The markdown as a single string.
