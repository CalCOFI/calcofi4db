# Build the species catalog record (`taxa.json`)

One entry per taxon of the release's `taxon` table with an observation
at or below it — the observed taxa plus their ancestors — with its
lineage, the ids, the groups it belongs to, its direct and rolled-up
observation counts, and one block per dataset that observed it carrying
that dataset's own name for it. The 204 vocabulary-only rows of
v2026.09.06 get no entry: they are listed under their dataset in
`datasets[].vocabulary_only[]`.

## Usage

``` r
build_taxa_catalog(con, record, release_version = NULL, release_date = NULL)
```

## Arguments

- con:

  a DBI connection holding the release tables `taxon`, `dataset_taxon`,
  `taxon_group` and `obs_bio` (and, optionally, `dataset`)

- record:

  the `datasets.json` record — a path or the list from
  [`build_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/build_dataset_catalog.md)
  — read for `datasets[].dataset_name_short`, `color` and `category`,
  and for the catalog order of `datasets[]`

- release_version:

  the release version (default: the record's)

- release_date:

  the release date, `YYYY-MM-DD` (default: the record's)

## Value

A list ready for
[`write_taxa_catalog()`](https://calcofi.io/calcofi4db/reference/write_taxa_catalog.md)
/ `jsonlite::write_json(auto_unbox = TRUE)`, validating against
`inst/schema/taxa.schema.json`.

## Details

Everything is read from the release: `taxon`, `dataset_taxon`,
`taxon_group` and `obs_bio` on `con`, the dataset names, colours and
categories from `record` (the `datasets.json` the release has just
written, so the dots on a species page and on a dataset page cannot
disagree). Nothing is authored and nothing is fetched.

Six grouped queries do the counting and every per-taxon lookup is a
split index, so the builder is linear in the number of `obs_bio` groups,
not quadratic in the number of taxa.

## See also

[`write_taxa_catalog()`](https://calcofi.io/calcofi4db/reference/write_taxa_catalog.md),
[`validate_taxa_catalog()`](https://calcofi.io/calcofi4db/reference/validate_taxa_catalog.md),
[`check_taxa_catalog()`](https://calcofi.io/calcofi4db/reference/check_taxa_catalog.md)
