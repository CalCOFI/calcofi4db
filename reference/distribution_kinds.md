# The controlled vocabularies of the dataset catalog registries

`distribution_kinds()` — what a `metadata/distribution.csv` row (and a
`distributions[]` entry) is: `download` (bytes you can fetch), `service`
(a queryable endpoint), `mirror` (the same rows served by someone else),
`source` (where the ingest read from), `archive` (a DOI-minting
deposit), and — record-only, derived — `page` (a calcofi.org page) and
`notebook` (the ingest). `distribution_portals()` — the host families a
URL is classified into by
[`classify_portal()`](https://calcofi.io/calcofi4db/reference/classify_portal.md).
`distribution_statuses()` — `current` (answers today), `superseded` (a
newer record exists: see `superseded_by`), `retired` (the authority no
longer serves it), `external` (a portal we do not run, listed as
declared) and `planned`. `registration_statuses()` — what a
`registrations[]` row says: `published`, `planned`, `n/a`.
`holding_statuses()` — a dataset without a release: `planned`,
`external`, `archived`. `visibility_values()` — `public` \| `internal`
(Decision 25: an internal dataset is in the record and flagged; every
public surface skips it).

## Usage

``` r
distribution_kinds()

distribution_portals()

distribution_statuses()

registration_statuses()

holding_statuses()

visibility_values()
```

## Value

Character vectors.
