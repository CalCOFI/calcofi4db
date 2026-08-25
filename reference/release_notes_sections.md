# Split RELEASES.md into its top-level sections

Split RELEASES.md into its top-level sections

## Usage

``` r
release_notes_sections(md)
```

## Arguments

- md:

  Character vector of lines, or a single string.

## Value

A tibble with `heading` (the `# ` line), `versions` (list of version
strings the heading names — one, or two for a range), `date` (from
`(YYYY-MM-DD)` if present), `body` (lines below the heading, trimmed).
