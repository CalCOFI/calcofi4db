# The licence a dataset's bundle carries, in words

Never invents a licence (workflows `CLAUDE.md`, Attribution):

## Usage

``` r
bundle_license(
  record,
  licenses_dir = system.file("licenses", package = "calcofi4db")
)
```

## Arguments

- record:

  one dataset record from `datasets.json`

- licenses_dir:

  the directory holding the legal texts

## Value

A list: `kind` (`standard` \| `public-domain` \| `custom` \|
`unstated`), `id` (the record's licence id, or NA), `summary` (one line
for the README) and `text` (the whole `LICENSE.txt`).

## Details

- a licence id with a bundled legal text (`inst/licenses/{id}.txt`:
  CC-BY-4.0, CC-BY-NC-4.0, CC-BY-SA-4.0, CC0-1.0) — `standard`: a short
  header, the provider's own statement URL where it differs from the
  licence's, then the full legal code;

- `US-PD` — `public-domain`: the 17 U.S.C. § 105 statement;

- `custom` — `custom`: the provider's terms URL, with no paraphrase of
  them;

- anything else, empty, `unknown` or an id with no text — `unstated`:
  says plainly that no licence has been established and whom to ask.
