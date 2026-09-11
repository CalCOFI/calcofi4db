# Turn `# Unreleased` into the section for a version being cut

If `# Unreleased` has a non-empty body it is renamed `# {version}` and a
fresh empty `# Unreleased` is inserted above it. If it is empty (or
absent) and no section for `version` exists, this errors: a release with
nothing to say about itself is the failure mode this file exists to
prevent.

## Usage

``` r
promote_unreleased(md, version, date = Sys.Date())
```

## Arguments

- md:

  RELEASES.md text (single string) or lines.

- version:

  the release being cut.

- date:

  accepted for compatibility; no longer written into the heading (the
  version string carries the date, `versions.json` the release_date).

## Value

The updated text as a single string.
