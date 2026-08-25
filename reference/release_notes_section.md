# The RELEASES.md section that documents a version

Matches a heading naming the version exactly, or a range heading
(`# v2026.08.04 – v2026.08.06`) that contains it.

## Usage

``` r
release_notes_section(md, version)
```

## Arguments

- md:

  RELEASES.md text or lines.

- version:

  e.g. `"v2026.08.25"`.

## Value

A one-row tibble as from
[`release_notes_sections()`](https://calcofi.io/calcofi4db/reference/release_notes_sections.md),
or `NULL`.
