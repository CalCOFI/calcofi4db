# Turn `# Unreleased` into the section for a version being cut

If `# Unreleased` has a non-empty body it is renamed
`# {version} ({date})` and a fresh empty `# Unreleased` is inserted
above it. If it is empty (or absent) and no section for `version`
exists, this errors: a release with nothing to say about itself is the
failure mode this file exists to prevent.

## Usage

``` r
promote_unreleased(md, version, date = Sys.Date())
```

## Arguments

- md:

  RELEASES.md text (single string) or lines.

- version, date:

  the release being cut.

## Value

The updated text as a single string.
