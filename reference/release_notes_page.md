# RELEASES.md as the public changelog page shows it

`RELEASES.md` is written for the release process, and two things about
it do not suit a reader of the rendered page (`RELEASES.html`). Its
`# Unreleased` heading has to stay in the source
([`promote_unreleased()`](https://calcofi.io/calcofi4db/reference/promote_unreleased.md)
renames it at the next cut) and is empty from the moment a release is
cut until the next change lands, so the page led with an empty section.
And a release's DOI is minted by Zenodo after its section is written, so
it is recorded in `catalog.json` and `versions.json`
([`publish_release_notes()`](https://calcofi.io/calcofi4db/reference/publish_release_notes.md))
and never in the notes. This returns the markdown the page is rendered
from:

## Usage

``` r
release_notes_page(md, versions = NULL)
```

## Arguments

- md:

  RELEASES.md text (single string) or lines.

- versions:

  the `versions` element of `versions.json` (a data frame, or a list of
  records, with `version` and `doi`), the parsed file itself, or `NULL`.

## Value

The markdown as a single string.

## Details

- an `# Unreleased` section with no entries is dropped, heading and all;
  one with entries is kept as it is;

- under each release heading, a `**DOI:**` line linking that release's
  own DOI (`https://doi.org/{doi}`) as `versions` records it. A release
  with no DOI gets no line. A range heading
  (`# v2026.08.04 – v2026.08.06`) lists the DOI of each version in its
  span that has one, each named by its version.

The source is never changed, and with `versions = NULL` only the empty
`# Unreleased` is dropped. Sections are split as
[`release_notes_sections()`](https://calcofi.io/calcofi4db/reference/release_notes_sections.md)
splits them, so "empty" here is the same test the release gate applies.

## Examples

``` r
md <- c("# Notes", "", "# Unreleased", "", "# v2026.11.09", "", "## More casts")
cat(release_notes_page(md, data.frame(
  version = "v2026.11.09",
  doi     = "10.5281/zenodo.24083894")))
#> # Notes
#> 
#> # v2026.11.09
#> 
#> **DOI:** [10.5281/zenodo.24083894](https://doi.org/10.5281/zenodo.24083894)
#> 
#> ## More casts
```
