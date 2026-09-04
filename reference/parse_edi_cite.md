# Parse a resolver's response into the fields the citation cache carries

Each parser takes the raw text of one response and returns a list; they
run on saved responses in the tests, so no network is needed to pin
them.

- `parse_edi_cite()` — EDI's cite service
  (`https://cite.edirepository.org/cite/<scope>.<id>.<rev>?style=ESIP`):
  the citation verbatim, plus the DOI and year it contains.

- `parse_erddap_das()` — an ERDDAP `.das`: the `NC_GLOBAL` string
  attributes as a named list (`title`, `institution`, `creator_name`,
  `license`, `citation` when a dataset declares one, …), multi-line
  values joined.

- `parse_ncei_landing()` — an NCEI landing page: its "Cite as:" block
  with the `[indicate subset used]` / `Accessed [date]` placeholders
  removed, plus the DOI.

- `parse_datacite()` — `https://api.datacite.org/dois/<doi>`: DOI,
  title, creators, publisher, year, URL and the SPDX `license` from
  `rightsList` (upper-cased to the registry's form, `CC-BY-4.0`).

- `parse_doi_bibliography()` — doi.org content negotiation
  (`Accept: text/x-bibliography; style=apa`): the formatted citation
  with markup and entities stripped.

## Usage

``` r
parse_edi_cite(x)

parse_erddap_das(x)

parse_ncei_landing(x)

parse_datacite(x)

parse_doi_bibliography(x)
```

## Arguments

- x:

  the response body, one string

## Value

A list (or, for `parse_doi_bibliography()`, one string).
